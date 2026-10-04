const {DatabaseSync}=require('node:sqlite');
const {Worker,isMainThread}=require('node:worker_threads');
const db=new DatabaseSync(':memory:');
db.exec('CREATE TABLE calibration (value INTEGER); INSERT INTO calibration VALUES (1);');
const statement=db.prepare('SELECT value FROM calibration');
statement.get(); statement.get(); statement.all(); [...statement.iterate()];
db.prepare('INSERT INTO calibration VALUES (?)').run(2);
db.close();
if(isMainThread) new Worker(__filename,{execArgv:[]});
