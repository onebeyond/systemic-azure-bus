# systemic-azure-bus
A systemic component to deal with azure bus topics and queues subscriptions and publications

A configuration example would be:

```json
{
  "bus": {
		"publications": {
			"emailSent": {
				"topic": "email-service.v1.email.sent"
			},
			"emailError": {
				"topic": "email-service.v1.email.error"
			}
		},
		"subscriptions": {
			"orderPlaced": {
				"topic": "klopotek.v1.order.confirmed",
				"subscription": "email.order_confirmed.send"
			}
		}
  }
}
```