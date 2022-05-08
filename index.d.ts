import { Component } from 'systemic'
import { ServiceBusMessage, ServiceBusReceivedMessage } from '@azure/service-bus'

/**
 * Configuration for the Systemic Azure Service Bus Component
 */
export type Config<TPublications extends Record<string, unknown>, TSubstriptions extends Record<string, unknown>> = {
  connection: {
    connectionString: string
  }
  publications?: Record<keyof TPublications, { topic: string; contentType: string }>
  subscriptions?: Record<keyof TSubstriptions, { topic: string; subscription: string }>
}

export type Bus<TPublications, TSubstriptions> = PublicationBus<TPublications> & SubscriptionBus<TSubstriptions>

export type PublicationBus<TPublications> = {
  /**
   * publishes a message on the bus
   */
  publish: <TTopic extends keyof TPublications>(
    publicationName: TTopic,
  ) => (message: TPublications[TTopic], options?: Omit<ServiceBusMessage, 'body'> & { label?: string, contentEncoding?: 'zlib' | 'default' }) => Promise<void>
}

export type SubscriptionBus<TSubstriptions> = {
  /**
   * subscribe to events on the bus
   * @returns A function that can be used to subribe to a topic on the bus, by providing topic name and a handler that
   * is invoked when a message is received.
   */
  subscribe: (
    onProcessError: (error: Error) => void,
  ) => <TTopic extends keyof TSubstriptions>(
      subscriptionName: TTopic,
      handler: (message: {
        body: TSubstriptions[TTopic]
        applicationProperties: ServiceBusReceivedMessage['applicationProperties']
        properties: Omit<ServiceBusReceivedMessage, 'body' | 'applicationProperties'>
      }) => Promise<void> | void,
    ) => void
}

/**
 * Initializes a Systemic Azure Service Bus Component.
 * @param {Config} config Configuration for the component
 */
declare function initBus<
  TPublications extends Record<string, unknown> = Record<string, unknown>,
  TSubstriptions extends Record<string, unknown> = Record<string, unknown>,
  >(): Component<Bus<TPublications, TSubstriptions>, { config: Config<TPublications, TSubstriptions> }>

export default initBus
