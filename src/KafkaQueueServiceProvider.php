<?php
/**
 * @copyright Copyright (c) 2018 Jinan Larva Information Technology Co., Ltd.
 * @link http://www.larvacent.com/
 * @license http://www.larvacent.com/license/
 */

namespace Larva\Queue\Kafka;

use Illuminate\Queue\QueueManager;
use Illuminate\Support\ServiceProvider;

/**
 * Class KafkaServiceProvider
 *
 * @author Tongle Xu <xutongle@gmail.com>
 */
class KafkaQueueServiceProvider extends ServiceProvider
{
    /**
     * Register the service provider.
     */
    public function register()
    {
        $this->mergeConfigFrom(
            __DIR__ . '/../config/kafka.php', 'queue.connections.kafka'
        );
        $this->registerDependencies();
    }

    /**
     * Register the application's event listeners.
     */
    public function boot()
    {
        /** @var QueueManager $queue */
        $queue = $this->app['queue'];
        $connector = new KafkaConnector($this->app);
        $queue->addConnector('kafka', function () use ($connector) {
            return $connector;
        });
    }

    /**
     * Register adapter dependencies in the container.
     */
    protected function registerDependencies()
    {
        $this->app->bind('queue.kafka.topic_conf', function () {
            return new \RdKafka\TopicConf();
        });
        $this->app->bind('queue.kafka.producer', function () {
            return new \RdKafka\Producer();
        });
        $this->app->bind('queue.kafka.conf', function () {
            return new \RdKafka\Conf();
        });
        $this->app->bind('queue.kafka.consumer', function ($app, $parameters) {
            return new \RdKafka\KafkaConsumer($parameters['conf']);
        });
    }

    /**
     * Get the services provided by the provider.
     *
     * @return array
     */
    public function provides()
    {
        return [
            'queue.kafka.topic_conf',
            'queue.kafka.producer',
            'queue.kafka.consumer',
            'queue.kafka.conf',
        ];
    }
}