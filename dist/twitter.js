"use strict";
// Search for Tweets within the past seven days
// https://developer.twitter.com/en/docs/twitter-api/tweets/search/quick-start/recent-search
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
const needle = require('needle');
// The code below sets the bearer token from your environment variables
// To set environment variables on macOS or Linux, run the export command below from the terminal:
// export BEARER_TOKEN='YOUR-TOKEN'
const token = process.env.BEARER_TOKEN;
const endpointUrl = "https://api.twitter.com/2/tweets/search/recent";
function getRecentTweets() {
    return __awaiter(this, void 0, void 0, function* () {
        const todayUTC = new Date();
        todayUTC.setUTCHours(0, 0, 0, 0);
        // Edit query parameters below
        // specify a search query, and any additional fields that are required
        // by default, only the Tweet ID and text fields are returned
        const params = {
            'query': '(#defi OR #crypto OR #cryptocurrency) is:retweet',
            'start_time': todayUTC.toISOString(),
            'tweet.fields': 'text,public_metrics,entities,referenced_tweets',
            'user.fields': 'description,public_metrics',
            'expansions': 'author_id,referenced_tweets.id',
            'max_results': 11
        };
        const res = yield needle('get', endpointUrl, params, {
            headers: {
                "User-Agent": "v2RecentSearchJS",
                "authorization": `Bearer ${token}`
            }
        });
        if (res.body) {
            return res.body;
        }
        else {
            throw new Error('Unsuccessful request');
        }
    });
}
exports.default = getRecentTweets;
//# sourceMappingURL=twitter.js.map