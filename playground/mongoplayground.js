const {MongoClient} = require('mongodb');

async function listDatabases(client) {
    databasesList = await client.db().admin().listDatabases();
    console.log("Databases:");
    databasesList.databases.forEach(db => console.log(` - ${db.name} `));
}

async function fuzzySearch(client) {
    const pipeline = [
        {
            $search: {
                "text": {
                    "path": "Md5",
                    "query": "1011000101111111000001110100000100110010001001000001111000011010", //Cipher v06 c11 - 054.jpg
                    "fuzzy": {
                        "maxEdits": 2,
                        "maxExpansions": 100,
                    }
                }
            }
        },
        {
            $limit: 10
        }
    ]

    let cursor = client.db("PathHash").collection("pathhash1").aggregate(pipeline)
    await cursor.forEach(
        res => {
            console.log(res)
        }
    )
}

async function main() {
    atlasusername = process.env.atlasuser;
    atlaspwd = process.env.atlaspwd;

    console.log(atlasusername)
    console.log(atlaspwd)

    const uri = `mongodb+srv://${atlasusername}:${atlaspwd}@inkling-cluster.jnpkxro.mongodb.net/?tls=true`;
    const client = new MongoClient(uri);

    try {
        await client.connect()
        await fuzzySearch(client)
    } catch (e) {
        console.error(e)
    } finally {
        await client.close()
    }
}

main().catch(console.error)