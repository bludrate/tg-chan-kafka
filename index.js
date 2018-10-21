const kafkaNode = require('kafka-node');
const fs = require('fs');
const offsetFileName = __dirname + '/__kafka-offsets.json';

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

function readFile() {
  return new Promise( ( resolve, reject ) => {
    fs.readFile( offsetFileName, ( error, content ) => {
      if ( error ) {
        reject( error );
      } else {
        resolve( JSON.parse( content ) );
      }
    } );
  } );
}

class Kafka {
  constructor(){
    this.client = new kafkaNode.Client();

    return readFile().then( offsets => {
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
        this.kafkaProducer.on('ready', () => {
          resolve( this.kafkaProducer );
        } );
        this.kafkaProducer.on('error', function (err) {
          console.log( err );
        });
      } );
    }

    return this.producerPromise;
  }

  /**
  */
  subscribe( topics, callback ){
    const topicConfig = topics.map( t => ( {
      topic: t,
      offset: this.offsets[ t ]
    } ) ) ;

    const consumer = new kafkaNode.Consumer(
        this.client,
        topicConfig,
        {
            autoCommit: false,
            fromOffset: true
        }
    );

    consumer.on('message', ( message ) => {
      callback( message );

      this.offsets[ message.topic ] = message.offset + 1;

      saveFile( this.offsets );
    });
  }
}

module.exports = Kafka;
