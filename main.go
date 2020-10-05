package main

import (
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/olekukonko/tablewriter"
	histogram "github.com/vividcortex/gohistogram"
	"math"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
)

var (
	awsRegion        = flag.String(`region`, `us-west-1`, `aws ec2 region`)
	awsRetries       = flag.Int(`retries`, 10, ``)
	cloudwatchMetric = flag.String(`cloudwatchMetric`, `CPUUtilization`, ``)
	cloudwatchUnit   = flag.String(`cloudwatchUnit`, `Percent`, ``)
	cloudwatchPeroid = flag.Int64(`cloudwatchPeroid`, 120, ``)
	cloudwatchAgg    = flag.String(`cloudwatchAgg`, `average`, `metric value aggregator`)
	metricThreshold  = flag.Float64(`threshold`, 30.0, `metric segmentation value`)
	metricInterval   = flag.Duration(`interval`, 24*time.Hour, `duration of time to subtract from current time`)
	numberDays       = flag.Int(`days`, 7, `Number of days of metrics`)
)

type Ec2Instance struct {
	Id, Type, Role, Env, Name string
	LaunchTime                time.Time
	CwSummaries               map[string]CwSeriesSummary
}

func newEc2Instance(ec2Instance *ec2.Instance) Ec2Instance {
	instance := Ec2Instance{
		Id:          *ec2Instance.InstanceId,
		Type:        *ec2Instance.InstanceType,
		LaunchTime:  *ec2Instance.LaunchTime,
		CwSummaries: make(map[string]CwSeriesSummary),
	}

	for _, tag := range ec2Instance.Tags {
		if tag == nil {
			continue
		}

		switch *tag.Key {
		case "Name", "name":
			instance.Name = *tag.Value
		case "Env", "env":
			instance.Env = *tag.Value
		case "Role", "role":
			instance.Role = *tag.Value
		}
	}

	if instance.Role == `` &&
		ec2Instance.IamInstanceProfile != nil &&
		*ec2Instance.IamInstanceProfile.Arn != `` {
		instance.Role = path.Base(*ec2Instance.IamInstanceProfile.Arn)
	}

	if instance.Name == `` {
		instance.Name = *ec2Instance.PublicDnsName
	}

	return instance
}

func newAwsCfg(region string, retries int) *aws.Config {
	return &aws.Config{
		Region:     aws.String(region),
		MaxRetries: aws.Int(retries)}
}

func newEc2Svc(region string, retries int) *ec2.EC2 {
	return ec2.New(session.New(), newAwsCfg(region, retries))
}

func newCloudWatchSvc(region string, retries int) *cloudwatch.CloudWatch {
	return cloudwatch.New(session.New(), newAwsCfg(region, retries))
}

type CwSeriesSummary struct {
	NumDatapoints, ThresholdDatapoints int
	Sum, Mean, Q95, StdDev             float64
}

func (s CwSeriesSummary) ThresholdPercentage() float64 {
	return float64(s.ThresholdDatapoints) / float64(s.NumDatapoints)
}

type GroupedCwSeriesSummary struct {
	NumInstances int
	CwSeriesSummary
}

func summarizeCloudWatchSeries(series *cloudwatch.GetMetricStatisticsOutput, threshold float64) CwSeriesSummary {
	summary := CwSeriesSummary{NumDatapoints: len(series.Datapoints)}
	hist := histogram.NewHistogram(20)

	for _, datapoint := range series.Datapoints {
		var value float64
		switch parseCloudwatchAgg(*cloudwatchAgg) {
		case `Average`:
			value = *datapoint.Average
		case `Maximum`:
			value = *datapoint.Maximum
		case `Minimum`:
			value = *datapoint.Minimum
		case `Sum`:
			value = *datapoint.Sum
		}

		hist.Add(value)
		summary.Sum += value
		if value > threshold {
			summary.ThresholdDatapoints += 1
		}
	}

	summary.Mean = hist.Mean()
	summary.Q95 = hist.Quantile(0.95)
	summary.StdDev = math.Sqrt(hist.Variance())

	return summary
}

func queryInstances(ec2Svc *ec2.EC2) ([]*ec2.Instance, error) {
	var instances []*ec2.Instance
	var token *string

	for {
		resp, err := ec2Svc.DescribeInstances(
			&ec2.DescribeInstancesInput{
				NextToken: token,
				Filters: []*ec2.Filter{
					{Name: aws.String(`instance-state-name`), Values: []*string{aws.String(`running`)}}}})
		// {Name: aws.String(`tag:role`), Values: []*string{aws.String(*awsRole)}}}})
		if err != nil {
			return nil, err
		}

		for _, resv := range resp.Reservations {
			instances = append(instances, resv.Instances...)
		}

		if resp.NextToken != nil {
			token = resp.NextToken
		} else {
			break
		}
	}

	return instances, nil
}

func newCloudWatchQuery(metric, unit string, instanceId string, start *time.Time, end *time.Time) *cloudwatch.GetMetricStatisticsInput {
	metrics := &cloudwatch.GetMetricStatisticsInput{
		Namespace:  aws.String("AWS/EC2"),
		MetricName: aws.String(metric),
		StartTime:  start,
		EndTime:    end,
		Unit:       aws.String(unit),
		Period:     aws.Int64(*cloudwatchPeroid),
		Statistics: []*string{aws.String(parseCloudwatchAgg(*cloudwatchAgg))},
		Dimensions: []*cloudwatch.Dimension{
			{Name: aws.String("InstanceId"), Value: aws.String(instanceId)},
		},
	}

	return metrics
}

func processInstances(cw *cloudwatch.CloudWatch, cwMetric, cwUnit string, threshold float64,
	instances []*ec2.Instance) ([]Ec2Instance, error) {
	var ec2Instances []Ec2Instance
	for _, instance := range instances {
		ec2Instance := newEc2Instance(instance)

		fmt.Println(`querying for cw metrics:`, ec2Instance.Id)

		for cycle := 0; cycle < *numberDays; cycle++ {
			startWindow := time.Duration(-(24 * (cycle + 1)))
			endWindow := time.Duration(-(24 * cycle))
			start := aws.Time(time.Now().Add((startWindow * time.Hour)))
			end := aws.Time(time.Now().Add(endWindow * time.Hour))
			instanceMetrics, err := cw.GetMetricStatistics(
				newCloudWatchQuery(cwMetric, cwUnit, ec2Instance.Id, start, end))
			if err != nil {
				return nil, err
			}
			// If the value is permmited
			var newMetrics = summarizeCloudWatchSeries(instanceMetrics, threshold)
			if val, ok := ec2Instance.CwSummaries[cwMetric]; ok {
				val.Mean = (val.Mean + newMetrics.Mean) / 2.0
				val.Q95 = (val.Q95 + newMetrics.Q95) / 2.0
				val.StdDev = (val.StdDev + newMetrics.StdDev) / 2.0
				val.Sum = (val.Sum + newMetrics.Sum) / 2.0
				val.NumDatapoints = val.NumDatapoints + newMetrics.NumDatapoints
				val.ThresholdDatapoints = val.ThresholdDatapoints + newMetrics.NumDatapoints
				ec2Instance.CwSummaries[cwMetric] = val
			} else {
				ec2Instance.CwSummaries[cwMetric] = newMetrics
			}
		}
		ec2Instances = append(ec2Instances, ec2Instance)
	}
	return ec2Instances, nil
}

func groupInstancesByField(
	instances []Ec2Instance,
	fieldFn func(Ec2Instance) string) map[string][]Ec2Instance {
	groupedInstances := make(map[string][]Ec2Instance)
	for _, instance := range instances {
		group := fieldFn(instance)
		groupedInstances[group] =
			append(groupedInstances[group], instance)
	}

	return groupedInstances
}

func summarizeGroupedCwSeries(
	metric string,
	groupedInstances map[string][]Ec2Instance) map[string]GroupedCwSeriesSummary {
	summaries := make(map[string]GroupedCwSeriesSummary)

	for group, instances := range groupedInstances {
		summary := GroupedCwSeriesSummary{NumInstances: len(instances)}

		var totalMean, totalQ95, totalStdDev float64
		for _, instance := range instances {
			summary.NumDatapoints += instance.CwSummaries[metric].NumDatapoints
			summary.ThresholdDatapoints += instance.CwSummaries[metric].ThresholdDatapoints

			summary.Sum += instance.CwSummaries[metric].Sum
			totalMean += instance.CwSummaries[metric].Mean
			totalQ95 += instance.CwSummaries[metric].Q95
			totalStdDev += instance.CwSummaries[metric].StdDev
		}

		summary.Mean = totalMean / float64(summary.NumInstances)
		summary.Q95 = totalQ95 / float64(summary.NumInstances)
		summary.StdDev = totalStdDev / float64(summary.NumInstances)
		summaries[group] = summary
	}

	return summaries
}

func renderGroupedAsciiSummary(groupField string, summaries map[string]GroupedCwSeriesSummary) {
	table := tablewriter.NewWriter(os.Stdout)

	if *cloudwatchUnit != `Percent` {
		table.SetHeader(
			[]string{groupField, `Instances`, `Sum`, `Mean`, `Q95`, `StdDev`, `Threshold`, `Raw`})
	} else {
		table.SetHeader(
			[]string{groupField, `Instances`, `Mean`, `Q95`, `StdDev`, `Threshold`, `Raw`})
	}

	floatToString := func(n float64) string {
		return strconv.FormatFloat(n, 'f', 4, 64)
	}

	var groups []string
	for group := range summaries {
		groups = append(groups, group)
	}
	sort.Strings(groups)

	for _, group := range groups {
		summary, pres := summaries[group]

		if pres {
			if *cloudwatchUnit != `Percent` {
				table.Append([]string{
					group,
					strconv.Itoa(summary.NumInstances),
					floatToString(summary.Sum),
					floatToString(summary.Mean),
					floatToString(summary.Q95),
					floatToString(summary.StdDev),
					strconv.Itoa(summary.ThresholdDatapoints),
					strconv.Itoa(summary.NumDatapoints)})
			} else {
				table.Append([]string{
					group,
					strconv.Itoa(summary.NumInstances),
					floatToString(summary.Mean),
					floatToString(summary.Q95),
					floatToString(summary.StdDev),
					strconv.Itoa(summary.ThresholdDatapoints),
					strconv.Itoa(summary.NumDatapoints)})
			}
		}
	}

	startWindow := time.Duration(-(24 * (*numberDays)))

	start := aws.Time(time.Now().Add((startWindow * time.Hour))).UTC().Format("2006-01-02")
	end := aws.Time(time.Now()).UTC().Format("2006-01-02")

	fmt.Println(
		fmt.Sprintf("Collected from %v to %v", start, end),
		` -- `,
		strings.Join([]string{
			*awsRegion,
			*cloudwatchMetric,
			*cloudwatchUnit,
			parseCloudwatchAgg(*cloudwatchAgg),
			floatToString(*metricThreshold)}, ` / `))
	table.Render()
}

func parseCloudwatchAgg(value string) string {
	switch strings.ToLower(value) {
	case `max`, `maximum`:
		return `Maximum`
	case `min`, `minimum`:
		return `Minimum`
	case `sum`:
		return `Sum`
	}

	return `Average`
}

func main() {
	flag.Parse()

	instances, err := queryInstances(
		newEc2Svc(*awsRegion, *awsRetries))
	if err != nil {
		panic(err)
	}

	cwSvc := newCloudWatchSvc(*awsRegion, *awsRetries)
	ec2Instances, err := processInstances(
		cwSvc,
		*cloudwatchMetric,
		*cloudwatchUnit,
		*metricThreshold,
		instances)
	if err != nil {
		panic(err)
	}

	renderGroupedAsciiSummary(
		`Type / Role`,
		summarizeGroupedCwSeries(
			*cloudwatchMetric,
			groupInstancesByField(
				ec2Instances,
				func(i Ec2Instance) string {
					return strings.Join([]string{i.Type, i.Role}, `/`)
				})))
}
