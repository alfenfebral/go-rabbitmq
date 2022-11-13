package services

import (
	"context"
	"fmt"
	"go-rabbitmq/utils"

	"github.com/streadway/amqp"
	trace_sdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	pkg_amqp "go-rabbitmq/pkg/amqp"
)

type TodoAMQPPublisher interface {
	Create()
}

type todoAMQPPublisher struct {
	tp      *trace_sdk.TracerProvider
	channel *amqp.Channel
}

func NewTodoAMQPService(tp *trace_sdk.TracerProvider, channel *amqp.Channel) TodoAMQPPublisher {
	return &todoAMQPPublisher{
		tp:      tp,
		channel: channel,
	}
}

// Create - publish amqp create
func (publisher *todoAMQPPublisher) Create() {
	ctx := context.Background()

	messageName := "todo.create"

	// Create a new span (child of the trace id) to inform the publishing of the message
	tr := publisher.tp.Tracer("amqp")
	spanName := fmt.Sprintf("AMQP - publish - %s", messageName)

	opts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindProducer),
	}

	ctx, span := tr.Start(ctx, spanName, opts...)
	defer span.End()

	q, err := publisher.channel.QueueDeclare(messageName, true, false, false, false, nil)
	if err != nil {
		utils.CaptureError(err)
	}

	// Inject the context in the headers
	headers := pkg_amqp.InjectAMQPHeaders(ctx)
	body := "Hello world!"
	msg := amqp.Publishing{
		Headers:     headers,
		ContentType: "text/plain",
		Body:        []byte(body),
	}
	err = publisher.channel.Publish("", q.Name, false, false, msg)
	if err != nil {
		utils.CaptureError(err)
	}
}
