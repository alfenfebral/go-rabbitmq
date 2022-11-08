package pkg_amqp

import (
	"go-rabbitmq/utils"

	"github.com/streadway/amqp"
)

func ConnectAmqp(url string) (*amqp.Channel, func() error) {
	connection, err := amqp.Dial(url)
	if err != nil {
		utils.CaptureError(err)
		return nil, nil
	}

	channel, err := connection.Channel()
	if err != nil {
		utils.CaptureError(err)
		return nil, nil
	}

	return channel, channel.Close
}
