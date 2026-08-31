# Telemetry in Taskmanager

The TaskManager application is equipped with OpenTelemetry capabilities.
That means that it supports the use of an OpenTelemetry java agent, even though it is not manually instrumented.

## Using metrics for autoscaling

To effectively use resources it is recommended to use automatic scaling of workers based on work load.
Scaling in this context means adding or removing nodes/containers/tasks (horizontal scaling).
When utilisation is low the system can be scaled down and when demand increases the system can be scaled up.
Metrics are reported by a 1 minute time frame.
Scaling can be triggered by monitoring metrics and when certain conditions are met for several minutes it can trigger scaling up or down.
Scaling up or down is typically done by increasing/decreasing the number of workers by a preset amount.
Setting the time span for scaling to low can result in erratic behavior either reducing performance because tasks are constantly redone
or will trigger blocks by the service provider.


> [!NOTE]
> There is no mechanism in place to control which workers to scale down if they are doing work or are idle.
> Meaning workers are arbitrarily shut down whether they are handling work or not.
> This is no problem as the task will just be put back on the queue.
> But when tasks take a long time this means all that work will be redone.
> Therefore it is recommended to use a scale down strategy which only scales down when no work is done for workers that handle long running tasks.

To control the scaling there are several metrics available both in the TaskManager as well as based on RabbitMQ metrics.
The metrics from the TaskManager are more precise as they calculate averages per minute.
The metrics from RabbitMQ are snapshots,
they require a bit more work to get the same information as the TaskManager metrics.
RabbitMQ metrics however can handle situations where the TaskManager restarts, see Note.

> [!NOTE]
> When the TaskManager is restarted it loses the state of the task running.
> This is no issue for the operation of the system because that doesn't depend on that information.
> However, it can affect auto scaling.
> Because after restart the TaskManager will report as if there are no tasks running.
> An autoscaling depending on these metrics might decide to scale down.
> This is not a problem, except if there are long running tasks these will be restarted,
> which might be inconvenient.

### Scaling strategies

There are different strategies to use for auto scaling.
Here 2 possible strategies are described.

#### Scale based on load

Scaling based on load means if the system reaches a certain percentage more workers are scaled up.
And if the percentage falls below another percentage workers are scaled down.
Scaling based on the Taskmanager `aer.taskmanager.work.load` metric is to be used.

It is also possible to scale based on the RabbitMQ metrics.
But that would require some additional calculation to get the load value.
To calculate the load use the following formula:
((`rabbitmq_detailed_queue_messages` - `rabbitmq_detailed_queue_messages_ready`) / `rabbitmq_detailed_queue_messages_consumers`) * 100.
The metric value `aerius.worker.<worker type>` for attribute `rabbitmq_queue` should be used for this.

Scaling on percentage works best when scaling percentages are close to 100%
or the total number of workers that can be scaled up to is not that high (below 100).
Because with the higher the amount of possible workers the higher the margin becomes.
For example if the scale down percentage is 70%,
and there are 300 workers running it won't scale down until there are less than 210 running.
This could mean 90 workers could be sitting idle for some time before they are scaled down.

#### Scale based on the amount of idle workers

Scaling based on idle workers means if there are less than a certain amount for idle workers the system scales up.
And if the amount of idle workers exceeds a certain value the system scales down.

To scale based on the RabbitMQ metrics the following calculation must be used.
To calculate the load use the following formula:
`rabbitmq_detailed_queue_messages_consumers`- (`rabbitmq_detailed_queue_messages` - `rabbitmq_detailed_queue_messages_ready`)
The metric value `aerius.worker.<worker type>` for attribute `rabbitmq_queue` should be used for this.

## Metrics

### TaskManager metrics

The TaskManager defines a few custom metrics for OpenTelemetry to capture.
These metrics are all defined with the `nl.aerius.TaskManager` instrumentation scope.
The metrics about availability of workers use the Open Telemetry standard for reporting such metrics.
Therefore `.limit` gives the total number of workers available.
And `.usage` gives the metrics about how the workers are used.
The usage metrics have an attribute `state` that identifies if the metric is for `used` or `free` amount of workers.
Additional `aer.rabbitmq.worker.usage` records a third state `waiting`.
And `aer.taskmanager.client.queue.usage` has states `used` and `waiting` that provides information on tasks send to the TaskManager.
This shows the number of tasks on the worker queue that are not yet picked up by any worker.


| metric name                                           | type      | description                                                                |
|-------------------------------------------------------|-----------|----------------------------------------------------------------------------|
| `aer.taskmanager.work.load`<sup>1</sup>               | gauge     | Percentage of workers occupied.                                            |
| `aer.taskmanager.work`<sup>1</sup>                    | gauge     | Count the number task processed on a worker.                               |
| `aer.taskmanager.work.duration`<sup>1</sup>           | gauge     | Average duration a task took to process, including wait time.              |
| `aer.taskmanager.work.queue`<sup>3</sup>              | gauge     | Count the number task from a queue processed on a worker.                  |
| `aer.taskmanager.work.queue.duration`<sup>3</sup>     | gauge     | Average duration a task from a queue took to process, including wait time. |
| `aer.taskmanager.worker.limit`<sup>1</sup>            | gauge     | Weighted total number of workers based on tasks send to workers.           |
| `aer.taskmanager.worker.usage`<sup>2</sup>            | gauge     | Weighted usage of workers based on tasks send to workers.                  |
| `aer.taskmanager.workerpool.worker.limit`<sup>1</sup> | gauge     | TaskManager internal total number of workers.                              |
| `aer.taskmanager.workerpool.worker.usage`<sup>2</sup> | gauge     | TaskManager internal usage of workers.                                     |
| `aer.taskmanager.client.queue.usage`<sup>2</sup>      | gauge     | TaskManager internal metrics on client queue usage.                        |
| `aer.rabbitmq.worker.limit`<sup>1</sup>               | gauge     | Total number of workers available as reported by RabbitMQ                  |
| `aer.rabbitmq.worker.usage`<sup>2</sup>               | gauge     | Usage o the workers based on the messages on the RabbitMQ worker queue.    |
| `aer.taskmanager.dispatched`<sup>1</sup>              | histogram | The number of tasks dispatched.                                            |
| `aer.taskmanager.dispatched.wait`<sup>1</sup>         | histogram | The average wait time of tasks dispatched.                                 |
| `aer.taskmanager.dispatched.queue`<sup>3</sup>        | histogram | The number of tasks dispatched per client queue.                           |
| `aer.taskmanager.dispatched.queue.wait`<sup>3</sup>   | histogram | The average wait time of tasks dispatched per client queue.                |

Basically there are 3 metric groups that report similar information.
First the `aer.taskmanager.worker.*` metrics are a weighted value based on when tasks are send to the workers.
These metrics should give the most accurate value about usage of the workers.
Second the `aer.taskmanager.workerpool.worker.*` metrics are the internally used values to base scheduling priorities on.
These metrics should be close to the other metrics, but can be used to detect issues in case the TaskManager doesn't seem to operate as expected.
Third the `aer.rabbitmq.worker.*`  metrics are the values as received from the RabbitMQ api.
These metrics could also be obtained by directly reading the RabbitMQ api, but specifically for the usage don't require additional logic to get the usage metrics.
In general these metrics should report the same values, but due to timing (e.g. the moment the measure is taken) there can be differences.

##### Metric attributes

The workers have different attributes to distinguish specific metrics.
* <sup>1</sup> have attribute `worker_type`.
* <sup>2</sup> have attribute `worker_type` and `state`. `state` can have the value `used`, `free` or `waiting`.
* <sup>3</sup> have attribute `worker_type` and `queue_name`.

`worker_type` is the type of worker, e.g. `ops`.
`queue_name` is the originating queue the task initially was put on, e.g. `...calculator_ui_small`.

> [!NOTE]
> The metrics in the TaskManager operate on a time frame of 1 minute.
> The size and load metrics calculate a weighted average within that time frame taking into account the time span of each measure point within the time frame.

### RabbitMQ metrics

Besides the RabbitMQ metrics reported by the TaskManager it is also possible to get the metrics from RabbitMQ directly.
To read those metrics the following metrics are available:

| metric name                                         | type  | description                                                        |
|-----------------------------------------------------|-------|--------------------------------------------------------------------|
| `rabbitmq_detailed_queue_messages`                  | gauge | Total number of messages on the queue, both picked up and waiting. |
| `rabbitmq_detailed_queue_messages_ready`            | gauge | Number of messages waiting to be picked up.                        |
| `rabbitmq_detailed_queue_messages_consumers`        | gauge | Total number of workers available.                                 |

Each of those metrics have an attribute `rabbitmq_queue`.
There are 2 groups of metrics that are values of the attribute.
First the queues from the TaskManager to the worker.
These have the pattern `aerius.worker.<worker type>`.
For scaling these are relevant.
Second the queues with work towards the TaskManager.
These have the pattern `aerius.<worker type>.<job type>`.

It will require some arithmetic to calculate the amount of tasks on the worker or idle workers because the metrics don't give that information directly.
Because each worker process represents 1 RabbitMQ consumer the metric `rabbitmq_detailed_queue_messages_consumers` can be used to measure the amount of workers available.

## Traces

When using the OpenTelemetry java agent, use of the RabbitMQ library within TaskManager should ensure that spans are created on task pickup/delivery.
Taskmanager will try to keep traces per task intact as much as possible, for instance when switching threads.
Taskmanager does not explicitly defines spans itself currently, it relies on the automatic spans created when using the java agent.

