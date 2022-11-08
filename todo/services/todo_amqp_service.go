package services

import (
	"context"
	"fmt"
	"go-rabbitmq/utils"

	"github.com/streadway/amqp"
	"go.opentelemetry.io/otel/sdk/trace"

	pkg_amqp "go-rabbitmq/pkg/amqp"
)

type TodoAMQPPublisher interface {
	Create()
}

type todoAMQPPublisher struct {
	Tp      *trace.TracerProvider
	channel *amqp.Channel
}

func NewTodoAMQPService(tp *trace.TracerProvider, channel *amqp.Channel) TodoAMQPPublisher {
	return &todoAMQPPublisher{
		Tp:      tp,
		channel: channel,
	}
}

// Create - publish amqp create
func (publisher *todoAMQPPublisher) Create() {
	ctx := context.Background()

	messageName := "todo.create"

	// Create a new span (child of the trace id) to inform the publishing of the message
	tr := publisher.Tp.Tracer("amqp")
	spanName := fmt.Sprintf("AMQP - publish - %s", messageName)
	ctx, span := tr.Start(ctx, spanName)
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
