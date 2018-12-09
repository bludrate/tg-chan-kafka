const kafkaNode = require('kafka-node');
const fs = require('fs');
const offsetFileName = process.cwd() + '/__kafka-offsets.json';

function saveFile( data ) {
  return new Promise( ( resolve, reject ) => {
    fs.writeFile(offsetFileName, JSON.stringify( data, null, 2 ), ( error, data ) => {
      if ( error ) {
        reject( error );
      } else {
        resolve();
      }
    } );
  } );
}

function getOffsets() {
  return new Promise( ( resolve, reject ) => {
    fs.readFile( offsetFileName, ( error, content ) => {
      if ( error ) {
        reject( error );
      } else {
        const contentString = content.toString();

        resolve( contentString ? JSON.parse( contentString ) : {} );
      }
    } );
  } );
}

class Kafka {
  constructor(){
    this.client = new kafkaNode.Client();

    return getOffsets()
      .then( offsets => {
        this.offsets = offsets;

        return this;
      } )
      .catch( ( error ) => {
        this.offsets = {};

        return this;
      } );
  }

  get producer(){
    if ( !this.kafkaProducer ) {
      this.kafkaProducer = new kafkaNode.Producer( this.client );
      this.producerPromise = new Promise( ( resolve, reject ) => {
        if ( this.kafkaProducer.ready ) {
          resolve( this.kafkaProducer );
        } else {
          this.kafkaProducer.on('ready', () => {
            resolve( this.kafkaProducer );
          } );
        }

        this.kafkaProducer.on('error', function (err) {
          console.log( err );
        });
      } );
    }

    return this.producerPromise;
  }

  getOffsets( topics ) {
    return new Promise( (resolve, reject ) => {
      const offset = new kafkaNode.Offset( this.client );

      offset.fetchLatestOffsets(topics, (error, offsets) => {
        if (error)
          return reject( error );

        for ( let topic in offsets ) {
          this.offsets[ topic ] = offsets[ topic ][0];
        }

        resolve( this.offsets );
      } );
    });
  }

  /**
  */
  async subscribe( topics, callback ){
    if ( topics.some( t => {
      return this.offsets[ t ] === undefined;
    } ) ) {
      await this.getOffsets( topics );
    }

    const topicConfig = topics.map( t => ({
      topic: t,
      offset: this.offsets[ t ] || 0
    }));

    const consumer = new kafkaNode.Consumer(
        this.client,
        topicConfig,
        {
            autoCommit: false,
            fromOffset: true
        }
    );

    consumer.on('message', ( message ) => {
      console.log(message);
      callback( message );

      this.offsets[ message.topic ] = message.offset + 1;

      saveFile( this.offsets );
    }).on('error', (err) => {
      if ( err.topics ) {
        console.log('create topics', err.topics);
        this.client.createTopics(err.topics, (error, result) => {
          if ( error ) {
            return console.log(error);
          }
          this.subscribe(topics, callback);
        });
      } else {
        console.log(err);
      }
    })
  }
}

module.exports = Kafka;
