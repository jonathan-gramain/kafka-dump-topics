'use strict';

const async = require('async');
const fs = require('fs');
const { ConsumerGroup } = require('kafka-node');

if (process.argv.length <= 3) {
    console.error('kafka-dump-topics - dump kafka topic messages into separate local log files\n' +
                  '\n' +
                  'usage: kafka-dump-topics.js zookeeper topic1 [topic2...]\n' +
                  '    zookeeper: kafka configured connection string to zookeeper\n' +
                  '               e.g. "localhost:2181"\n' +
                  '    topic1 [topic2...]: kafka topic names to dump to local log files\n');
    process.exit(1);
}

const connectionString = process.argv[2];
const dumperVars = process.argv.slice(3).map(topic => ({
    topic,
    outFile: `./${topic}.log`,
    nbConsumedMessages: 0,
}));

console.log('initializing consumers...');

async.each(dumperVars, (vars, done) => {
    vars.stream = fs.createWriteStream(vars.outFile, { flags: 'a' });
    vars.consumer = new ConsumerGroup({
        host: connectionString,
        groupId: 'debugging-consumer',
        fromOffset: 'latest',
        autoCommit: true,
        fetchMaxBytes: 100000,
    }, vars.topic);
    vars.consumer.on('error', err => {
        console.error(`error in consumer of ${vars.topic} topic: ${err.message}`);
    });
    vars.consumer.once('connect', () => {
        console.log(`    ${vars.topic} => ${vars.outFile}`);
        vars.consumer.on('message', entry => {
            vars.stream.write(`${Date.now()},${entry.value}\n`, 'utf8');
            ++vars.nbConsumedMessages;
        });
        done();
    });
}, () => {
    process.on('SIGINT', () => {
        console.log('closing consumers and log files...');
        async.each(dumperVars, (vars, done) => {
            vars.consumer.close(true, () => {
                console.log(`    ${vars.outFile}: ${vars.nbConsumedMessages} messages dumped`);
                vars.stream.on('finish', done);
                vars.stream.end();
            });
        }, () => {
            process.exit(0);
        });
    });
});
