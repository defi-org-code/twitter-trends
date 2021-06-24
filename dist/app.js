"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const http_1 = require("http");
const twitter_1 = __importDefault(require("./twitter"));
const better_sqlite3_1 = __importDefault(require("better-sqlite3"));
const types_1 = require("./types");
const fs = require('fs');
let db;
const port = 5000;
const server = http_1.createServer((req, res) => __awaiter(void 0, void 0, void 0, function* () {
    if (req.url === '/index.html') {
        res.writeHead(200, {
            'Content-Type': 'text/html'
        });
        fs.readFile('./index.html', null, function (error, data) {
            if (error) {
                res.writeHead(404);
                res.write('Whoops! File not found!');
            }
            else {
                res.write(data);
            }
            res.end();
        });
        return;
    }
    else if (req.url === '/get-trends') {
        res.setHeader('Content-Type', 'application/json');
        const result = fs.readFileSync('./top-entities.json', 'utf8');
        res.end(result);
        return;
    }
    res.end('Hello world!');
}));
server.listen(port, () => __awaiter(void 0, void 0, void 0, function* () {
    console.log(`Server listening on port ${port}`);
    db = better_sqlite3_1.default('./twitter.db');
    db.exec('CREATE TABLE IF NOT EXISTS tweets (id TEXT PRIMARY KEY, count INTEGER)');
    db.exec('CREATE TABLE IF NOT EXISTS entities (name TEXT, type INTEGER, count INTEGER, processed INTEGER, lastUpdateTime TEXT, PRIMARY KEY (name, type))');
    console.log("db");
    yield truncateEntities();
    yield processTweets();
    // setInterval(async () => {
    //     await processTweets();
    // }, 30000);
}));
const processTweets = () => __awaiter(void 0, void 0, void 0, function* () {
    console.log("process tweets", new Date());
    const response = yield twitter_1.default();
    const tweets = response.includes.tweets.map((t) => {
        return { id: t.id, entities: t.entities, public_metrics: t.public_metrics };
    });
    for (const t of tweets) {
        t.counterToUpdate = yield updateTweet(t);
    }
    yield setProcessedEntities();
    yield updateEntities(tweets);
    yield writeTopEntitiesToDisk();
    console.log("Finished processing tweets", new Date());
});
const updateTweet = (tweet) => __awaiter(void 0, void 0, void 0, function* () {
    let count = tweet.public_metrics.retweet_count + tweet.public_metrics.quote_count;
    const prev = db.prepare('select * from tweets where id = ?').get(tweet.id);
    if (!prev) {
        db.prepare('insert into tweets values (?,?)').run(tweet.id, count);
        return count;
    }
    else {
        db.prepare('update tweets set count = ? where id = ?').run(count, tweet.id);
        return count - prev.count;
    }
});
const setProcessedEntities = () => __awaiter(void 0, void 0, void 0, function* () {
    console.log('setProcessedEntities');
    return db.prepare('update entities set processed = count, lastUpdateTime = ? where not IFNULL(processed, -1) = count').run(new Date().toUTCString());
});
const updateEntities = (tweets) => __awaiter(void 0, void 0, void 0, function* () {
    const time = new Date();
    const entities = [];
    tweets.forEach((t) => {
        if (t.entities.cashtags) {
            t.entities.cashtags.forEach(cashtag => {
                const entity = entities.find(e => e.name === cashtag.tag && e.type === types_1.EntityType.CASHHASH);
                if (entity) {
                    entity.count += t.counterToUpdate;
                }
                else {
                    entities.push({
                        type: types_1.EntityType.CASHHASH,
                        name: cashtag.tag,
                        count: t.counterToUpdate,
                        lastUpdateTime: time
                    });
                }
            });
        }
        if (t.entities.hashtags) {
            t.entities.hashtags.forEach(hashtag => {
                const entity = entities.find(e => e.name === hashtag.tag && e.type === types_1.EntityType.HASHTAG);
                if (entity) {
                    entity.count += t.counterToUpdate;
                }
                else {
                    entities.push({
                        type: types_1.EntityType.HASHTAG,
                        name: hashtag.tag,
                        count: t.counterToUpdate,
                        lastUpdateTime: time
                    });
                }
            });
        }
        if (t.entities.mentions) {
            t.entities.mentions.forEach(mention => {
                const entity = entities.find(e => e.name === mention.username && e.type === types_1.EntityType.MENTION);
                if (entity) {
                    entity.count += t.counterToUpdate;
                }
                else {
                    entities.push({
                        type: types_1.EntityType.MENTION,
                        name: mention.username,
                        count: t.counterToUpdate,
                        lastUpdateTime: time
                    });
                }
            });
        }
        if (t.entities.urls) {
            t.entities.urls.forEach(url => {
                const entity = entities.find(e => e.name === url.url && e.type === types_1.EntityType.URL);
                if (entity) {
                    entity.count += t.counterToUpdate;
                }
                else {
                    entities.push({
                        type: types_1.EntityType.URL,
                        name: url.url,
                        count: t.counterToUpdate,
                        lastUpdateTime: time
                    });
                }
            });
        }
    });
    const entitiesStatement = db.prepare('Insert INTO entities(type,name,count,lastUpdateTime) values (?,?,?,?)\n' +
        'ON CONFLICT (type,name) DO UPDATE SET count = count + ?, lastUpdateTime = ?');
    console.log("entities", entities.map(e => e.name));
    db.transaction((entities) => {
        entities.forEach(entity => {
            entitiesStatement.run(entity.type, entity.name, entity.count, entity.lastUpdateTime.toUTCString(), entity.count, entity.lastUpdateTime.toUTCString());
        });
    })(entities);
    console.log("done");
});
const writeTopEntitiesToDisk = () => __awaiter(void 0, void 0, void 0, function* () {
    const topEntities = yield fetchTopEntities();
    fs.writeFileSync('top-entities.json', JSON.stringify(topEntities), 'utf8');
});
const fetchTopEntities = () => __awaiter(void 0, void 0, void 0, function* () {
    const hashtags = db.prepare('select processed, count, name, lastUpdateTime from entities where type = ? order by processed desc, count desc limit 100').all(types_1.EntityType.HASHTAG);
    const cashtags = db.prepare('select processed, count, name, lastUpdateTime from entities where type = ? order by processed desc, count desc limit 100').all(types_1.EntityType.CASHHASH);
    const mentions = db.prepare('select processed, count, name, lastUpdateTime from entities where type = ? order by processed desc, count desc limit 100').all(types_1.EntityType.MENTION);
    const urls = db.prepare('select processed, count, name, lastUpdateTime from entities where type = ? order by processed desc, count desc limit 100').all(types_1.EntityType.URL);
    return {
        hashtags,
        cashtags,
        mentions,
        urls
    };
});
const truncateEntities = () => __awaiter(void 0, void 0, void 0, function* () {
    db.prepare('DELETE FROM entities').run();
});
//# sourceMappingURL=app.js.map