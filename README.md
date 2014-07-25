## Aftership Challenge II

Implemented a token bucket which has a capacity of 20 and refill rate of 2 per second for the 300 sample http requests to 3 couriers (DPDUK, HKPOST, USPS) to fetch data of 300 sample trackings.

#### Instructions

To see the server in action:

- fork / clone the repo to your own machine
- do `npm install` in command line to get all dependencies
- do `mongod` (or `sudo mongod`) in command line to start MongoDB server on local machine
- do `beanstalkd` to start beanstalkd server
- execute `node index.js` in the repo directory to start the requests! (you can see the jobs being put / destroyed in console.logs)

The tracking numbers with valid data will be stored in the local MongoDB database `aftership`, which those with invalid data / errors will be executed as a new job again afer 3 hours.

To access the data in MongoDB database:

- do `mongo` in command line to open the Mongo shell
- type `use aftership` to use the database used by the server
- do the query `db.trackings.find().pretty()` to see data of all successful tracking results!
