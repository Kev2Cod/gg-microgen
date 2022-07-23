/**
 * Responds to HTTP request.
 *
 * @param { app, context, callback }
 * app { getRequester, getPublisher }
 * context { body, cookies, method, params, query, headers }
 * callback(error, response) 
 */

const { Client } = require('@elastic/elasticsearch')
const { createClient } = require("redis")
const axios = require('axios')

const es = new Client({
    node: 'http://10.207.26.1:9200',
})

const redis = createClient({ url: 'redis://admin:secret@localhost:6379' })

const ES_INDEX = 'dev_gg_survey_responses_all_field_new'
const SIZE = 10
const EXPIRED = 3600
const initialKey = 'gg[1-9]'
const prefixKey = 'gg'

function generateNextToken(sort) {
    nextSearch = `${sort[0]}-${sort[1] + SIZE}`
    return Buffer.from(nextSearch).toString('base64')
}


const insertToRedis = async (searchAfter) => {
    try {
        const pit = await es.openPointInTime({ index: ES_INDEX, keep_alive: '1m' })

        const reqArg = {
            query: {
                match_all: {}
            },
            pit: {
                id: pit.id,
                keep_alive: '1m'
            },
            sort: [
                {
                    "_score": "desc"
                },
                {
                    "_shard_doc": "asc"
                }
            ],
        }

        let key
        let value

        if (!searchAfter) {
            const firstSearch = await es.search(reqArg)
            searchAfter = firstSearch.hits.hits.at(-1).sort
            key = searchAfter.join('-')
            value = JSON.stringify({ data: firstSearch.hits.hits.map(val => val._source), nextToken: generateNextToken(searchAfter) })
            await redis.set(`${prefixKey}[${key}]`, value, { EX: EXPIRED })
            console.log(`Process send data gg[${key}] Success`)

            axios.get(`http://localhost:3000/api/all-execute-api?next_token=${generateNextToken(searchAfter)}`).catch(err => console.log("Error: ", err))
            return
        } else {
            console.log(`Proses data gg[${searchAfter}] Running`)
            const nextSearchCheck = await es.search({
                ...reqArg,
                size: SIZE,
                search_after: [searchAfter[0], searchAfter[1] - SIZE]
            })
            // console.log("CHECK DATA LENGTH : ", nextSearchCheck.hits.hits.length)

            if (nextSearchCheck.hits.hits.length === 0) {
                console.log(' - selesai melakukan insert data')
                return
            } else {
                const nextSearch = await es.search({
                    ...reqArg,
                    size: searchAfter[1],
                })
                searchAfter = nextSearch.hits.hits.at(-1).sort
                searchAfter = [searchAfter[0], searchAfter[1] + 1]
                key = searchAfter.join('-')
                value = JSON.stringify({ data: nextSearch.hits.hits.map(val => val._source), nextToken: generateNextToken(searchAfter) })
                console.log("Data send Redis being process 🔃")
                await redis.set(`${prefixKey}[${key}]`, value, { EX: EXPIRED })
                console.log("Data send to Redis success ✔")

                axios.get(`http://localhost:3000/api/all-execute-api?next_token=${generateNextToken(searchAfter)}`).catch(err => console.log("Error :", err))
                return
            }
        }

    } catch (error) {
        console.log(error)
        return error
    }
}

exports.esToRedisWithApi = async (req, res) => {
    // check if redis is connected
    try {
        await redis.ping()
    } catch (e) {
        await redis.connect()
    }

    // let result
    const nextToken = req.headers['next_token'] || req.query['next_token']

    try {
        console.log(await redis.ping())

        if (!nextToken) {
            await insertToRedis()
        } else {
            const decodeToken = Buffer.from(nextToken, 'base64').toString('utf-8')

            if (!await redis.exists(`${prefixKey}[${decodeToken}]`)) {
                await insertToRedis(decodeToken.split('-'))
            } else {
                console.log(`Data gg[${decodeToken}] already exist in Redis`)
                let decodeNextToken = Buffer.from(nextToken, 'base64').toString('utf-8').split('-')
                decodeNextToken = decodeNextToken.map(val => parseInt(val))
                axios.get(`http://localhost:3000/api/all-execute-api?next_token=${generateNextToken(decodeNextToken)}`).catch(err => console.log("Error: ", err))
            }
            // result = JSON.parse(await redis.get(`${prefixKey}[${decodeToken}]`))
        }

        const response = {
            statusCode: 200,
            status: "Update data running..."
        }

        //  callback(null, response);
        res.send(response)

    } catch (error) {
        //  return callback(null, error)
        console.log(error)
        return res.send(error)
    }
}
