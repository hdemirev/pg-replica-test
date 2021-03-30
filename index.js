const { Pool } = require("pg");
const async = require("async");

const pool = new Pool({
  max: process.env.MAX_POOL_SIZE || 10,
  connectionString: process.env.DATABASE_URL,
});

const replicaPool = new Pool({
  max: process.env.MAX_POOL_SIZE || 10,
  connectionString: process.env.REPLICA_DATABASE_URL,
});

const init = async () => {
  try {
    await pool.query(
      "create table if not exists dummy_data(key text value text)"
    );
    console.log("table created");
  } catch (e) {
    console.log(e);
  }
};

const checkExistsOnReplica = async (key, value) => {
  const replicaResult = await pool.query(
    `select * from dummy_data where key = $1`,
    [key]
  );
  if (replicaResult.rows.length == 1) {
    return replicaResult.rows[0].value === value;
  } else {
    return false;
  }
};

let tasksDone = 0;
let replicaMatches = 0;

const logProgress = () => {
  console.log(
    "tasks completed:",
    tasksDone,
    "replica matches: ",
    replicaMatches
  );
};

const generateInserts = (numInserts) => {
  const tasks = [];
  for (var i = 0; i <= numTasks; i++) {
    const key = `${Math.random()}${Math.random()}`;
    const value = `${Math.random()}${Math.random()}`;
    const task = (done) => {
      pool
        .query("insert into dummy_data(key, value) values($1, $2)", [
          key,
          value,
        ])
        .then(() => {
          setTimeout(() => {
            checkExistsOnReplica(key, value).then((exists) => {
              if (exists) {
                replicaMatches++;
              }
            });
          }, process.env.REPLICATION_CHECK_TIME || 1000);

          tasksDone++;
          if (tasksDone % 100 === 0) {
            logProgress();
          }
          done();
        })
        .catch((e) => {
          console.log(e);
          done();
        });
    };
    tasks.push(task);
  }

  return tasks;
};

const runInserts = (inserts) => {
  async.parallelLimit(inserts, process.env.PARALLEL_LIMIT || 15, () => {
    console.log("all done with inserts");
    logProgress();
  });
};

const main = async () => {
  init();
  const inserts = generateInserts(process.env.NUM_INSERTS || 10000);
  runInserts(inserts);
};

main();