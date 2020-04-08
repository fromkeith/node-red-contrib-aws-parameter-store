const aws = require('aws-sdk');

const CACHE_AGE = 1000 * 60 * 30;

class SimpleCache {
    constructor() {
        this.cache = {};
        setInterval(() => {
            const keys = Object.keys(this.cache);
            const now = Date.now();
            for (const k of keys) {
                if (now - this.cache[k].at > CACHE_AGE) {
                    delete this.cache[k];
                }
            }
        }, CACHE_AGE / 2);
    }
    set(path, value) {
        this.cache[path] = {
            value,
            at: Date.now(),
        };
    }
    get(path) {
        const entry = this.cache[path];
        if (!entry) {
            return undefined;
        }
        if (Date.now() - entry.at > CACHE_AGE) {
            delete this.cache[path];
            return undefined;
        }
        return entry.value;
    }
}

module.exports = (RED) => {
    const mem = new SimpleCache();
    let ssm = new aws.SSM({
        region: RED.settings.awsParameterStoreRegion,
    });
    let lastRegion = RED.settings.awsParameterStoreRegion;

    function fetchPath(node, next) {
        return new Promise((resolve, reject) => {
            ssm.getParametersByPath({
                Path: node.keyPath,
                Recursive: true,
                WithDecryption: node.decrypt,
                NextToken: next,
            }, (err, resp) => {
                if (err) {
                    reject(err);
                    return;
                }
                resolve(resp);
            });
        }).then((resp) => {
            const result = {};
            for (const p of resp.Parameters) {
                result[p.Name] = p.Value;
            }
            if (resp.NextToken) {
                return fetchPath(node, resp.NextToken)
                    .then((subResult) => {
                        return Object.assign(result, subResult);
                    });
            }
            return result;
        });
    }

    function AwsParmeterStore(config) {
        RED.nodes.createNode(this, config);
        this.keyPath = config.keyPath;
        this.cache = config.cache;
        this.isPrefix = config.isPrefix;
        this.decrypt = config.decrypt;
        const cacheKey = `${this.keyPath}-${this.isPrefix}-${this.decrypt}`;
        this.on('input', (msg, send, done) => {
            // detect parameter store region change.
            if (RED.settings.awsParameterStoreRegion !== lastRegion) {
                ssm = new aws.SSM({
                    region: RED.settings.awsParameterStoreRegion,
                });
            }
            if (this.cache) {
                const val = mem.get(cacheKey);
                if (val) {
                    send({
                        payload: val,
                    });
                    return;
                }
            }
            if (this.isPrefix) {
                fetchPath(this).then((result) => {
                    if (this.cache) {
                        mem.set(cacheKey, result);
                    }
                    send({
                        payload: result,
                    });
                }, (err) => {
                    done(err || 'Unknown error');
                });
            } else {
                ssm.getParameter({
                    Name: this.keyPath,
                    WithDecryption: this.decrypt,
                }, (err, resp) => {
                    if (err) {
                        done(err);
                        return;
                    }
                    const result = {
                        [resp.Parameter.Name]: resp.Parameter.Value,
                    };
                    if (this.cache) {
                        mem.set(cacheKey, result);
                    }
                    send({
                        payload: result,
                    });
                });
            }
        });
    }

    RED.nodes.registerType("aws-parameter-store", AwsParmeterStore, {
        settings: {
            awsParameterStoreRegion: {
                value: 'us-west-2',
                exportable: true,
            },
        },
    });
};
