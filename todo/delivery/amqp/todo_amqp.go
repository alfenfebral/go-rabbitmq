package amqp_delivery

import (
	"context"
	"go-rabbitmq/todo/services"
	"go-rabbitmq/utils"

	pkg_amqp "go-rabbitmq/pkg/amqp"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	trace_sdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

type TodoAMQPConsumer interface {
	Create()
}

// todoAMQPConsumer represent the amqp
type todoAMQPConsumer struct {
	tp          *trace_sdk.TracerProvider
	todoService services.TodoService
	channel     *amqp.Channel
}

// NewTodoAMQPConsumer - make amqp consumer
func NewTodoAMQPConsumer(tp *trace_sdk.TracerProvider, channel *amqp.Channel, service services.TodoService) TodoAMQPConsumer {
	consumer := &todoAMQPConsumer{
		tp:          tp,
		todoService: service,
		channel:     channel,
	}
	consumer.Create()

	return consumer
}

// Create - create todo consumer
func (consumer *todoAMQPConsumer) Create() {
	messageName := "todo.create"

	q, err := consumer.channel.QueueDeclare(messageName, true, false, false, false, nil)
	if err != nil {
		utils.CaptureError(err)
	}

	msgs, err := consumer.channel.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		utils.CaptureError(err)
	}

	for d := range msgs {
		ctx := pkg_amqp.ExtractAMQPHeaders(context.Background(), d.Headers)

		tr := consumer.tp.Tracer("amqp")
		opts := []trace.SpanStartOption{
			trace.WithSpanKind(trace.SpanKindConsumer),
		}
		_, span := tr.Start(ctx, "AMQP - consume - todo.create", opts...)

		logrus.Printf("Received a message: %s", d.Body)

		err := d.Ack(false)
		if err != nil {
			utils.CaptureError(err)
		}

		span.End()
	}
}
