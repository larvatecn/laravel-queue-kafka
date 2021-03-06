# laravel-queue-kafka

This is a queue adapter for the Kafka.
修改自 https://github.com/rapideinternet/laravel-queue-kafka

## Installation

```bash
composer require larva/laravel-queue-kafka
```

## for Laravel

This service provider must be registered.

```php
// config/app.php

'providers' => [
    '...',
    Larva\Queue\Kafka\KafkaServiceProvider::class,
];
```

edit the config file: config/queue.php

add config

```php
        'kafka' => [
            /*
             * Driver name
             */
            'driver' => 'kafka',
            /*
             * The name of default queue.
             */
            'queue' => env('KAFKA_QUEUE', 'default'),
            /*
             * The group of where the consumer in resides.
             */
            'consumer_group_id' => env('KAFKA_CONSUMER_GROUP_ID', 'laravel_queue'),
            /*
             * Address of the Kafka broker
             */
            'brokers' => env('KAFKA_BROKERS', 'localhost'),
            /*
             * Determine the number of seconds to sleep if there's an error communicating with kafka
             * If set to false, it'll throw an exception rather than doing the sleep for X seconds.
             */
            'sleep_on_error' => env('KAFKA_ERROR_SLEEP', 5),
            /*
             * Sleep when a deadlock is detected
             */
            'sleep_on_deadlock' => env('KAFKA_DEADLOCK_SLEEP', 2),
            
            /*
             * 全局默认配置
             */
            'defaultConf' => [
                'enable.auto.commit' => 'false',
                'offset.store.method' => 'broker',
                //'security.protocol' => 'SASL_SSL',
                //'sasl.mechanisms' => 'PLAIN',
                //'sasl.username' => '',
                //'sasl.password' => '',
                //'ssl.ca.location' => '/ca-cert.pem',
            ]
        ],
```

change default to mns

```php
    'default' => 'kafka'
```

## Use

see [Laravel wiki](https://laravel.com/docs/5.7/queues)
