/*!
 * Connect - Google Data Store
 * Copyright(c) 2017 Keshav Channa
 * MIT Licensed

 https://github.com/kchanna/connect-google-datastore
 */

const Datastore = require('@google-cloud/datastore');
const util = require('util');
const noop = function(){};


/**
 * Return the `GoogleDataStore` extending `express`'s session Store.
 *
 * @param {object} express session
 * @return {Function}
 * @api public
 */

module.exports = function (session) {

  /**
   * Express's session Store.
   */

  var Store = session.Store;

  /**
   * Initialize GoogleDataStore with the given `options`.
   *
   * @param {Object} options
   * @api public
   */

  function GoogleDataStore (options) {
    if (!(this instanceof GoogleDataStore)) {
      throw new TypeError('Cannot call GoogleDataStore constructor as a function');
    }

    var self = this;

    this.verbose = options.verbose || 100;

    if(this.verbose > 90) {
      console.log("GoogleDataStore inited, with options: " + util.inspect(options));
    }

    options = options || {};
    Store.call(this, options);
    this.prefix = options.prefix == null
      ? 'session_of_users:'
      : options.prefix;

    delete options.prefix;

    this.scanCount = Number(options.scanCount) || 100;
    delete options.scanCount;

    this.serializer = options.serializer || JSON;

    this.ttl = options.ttl;
    this.disableTTL = options.disableTTL;
  }

  /**
   * Inherit from `Store`.
   */

  util.inherits(GoogleDataStore, Store);

  /**
   * Attempt to fetch session by the given `sid`.
   *
   * @param {String} sid
   * @param {Function} fn
   * @api public
   */

  GoogleDataStore.prototype.get = function (sid, fn) {
    if (!fn) fn = noop;

    if(this.verbose > 90) {
      console.log("GoogleDataStore get, sid: " + sid);
    }

    const datastore = Datastore();
    const theKey = datastore.key([this.prefix, sid]);

    datastore.get(theKey)
      .then((results) => {
        if(this.verbose > 95) {
          console.log("GoogleDataStore get, Got result for sid: " + sid + ", result = " + JSON.stringify(results));
        }

        const records = results[0];
        let resultt = null;

        if(!results || !records || records.length < 1) {
          //return fn(new Error("No sid record found"));
          return fn();
        }

        try {
          resultt = this.serializer.parse(records.sess);
        }
        catch (er) {
          return fn(er);
        }

        if(this.verbose > 95) {
          console.log("GoogleDataStore get, Sending back: " + JSON.stringify(resultt) + ", to session...");
        }

        fn(null, resultt);
      }).catch((err) => {
        console.error('GoogleDataStore get: ERROR:', err);
        return fn(err);
      });
  };

  /**
   * Commit the given `sess` object associated with the given `sid`.
   *
   * @param {String} sid
   * @param {Session} sess
   * @param {Function} fn
   * @api public
   */

  GoogleDataStore.prototype.set = function (sid, sess, fn) {
    var store = this;
    if (!fn) fn = noop;

    if(this.verbose > 90) {
      console.log("GoogleDataStore set, sid: " + sid + ", session = " + JSON.stringify(sess));
    }

    try {
      var jsess = store.serializer.stringify(sess);
    }
    catch (er) {
      return fn(er);
    }


    const datastore = Datastore();
    const theKey = datastore.key([store.prefix, sid]);
    const entity = {
      key: theKey,
      data: [
        {
          name: 'sid',
          value: sid
        },
        {
          name: 'sess',
          value: jsess
        },
        {
          name: 'lastUsed',
          value: new Date().toJSON()
        }
      ]
    };

    datastore.upsert(entity)
      .then(() => {
        if(this.verbose > 40) {
          console.log("GoogleDataStore: set key: " + JSON.stringify(theKey) + "-- saved successfully - for sid " + sid);
        }
        fn.apply(null, arguments);
      })
      .catch((err) => {
        console.log("GoogleDataStore: set key: " + JSON.stringify(theKey) + "-- ERROR:: " + JSON.strigify(err));
        return fn(er);
    });
  };

  /**
   * Destroy the session associated with the given `sid`.
   *
   * @param {String} sid
   * @api public
   */

  GoogleDataStore.prototype.destroy = function (sid, fn) {
    if(this.verbose > 90) {
      console.log("GoogleDataStore delete, sid: " + sid);
    }

    const datastore = Datastore();
    const theKey = datastore.key([this.prefix, sid]);

    datastore.delete(theKey)
      .then(() => {
        if(this.verbose > 40) {
          console.log(`GoogleDataStore: delete ${theKey.id} saved successfully - for sid ${sid}.`);
        }
        fn.apply(null, arguments);
      })
      .catch((err) => {
        console.log(`GoogleDataStore: delete ${theKey.id} ERROR:: ` + JSON.strigify(err));
        return fn(er);
    });
  };

  /**
   * Refresh the time-to-live for the session with the given `sid`.
   *
   * @param {String} sid
   * @param {Session} sess
   * @param {Function} fn
   * @api public
   */

  GoogleDataStore.prototype.touch = function (sid, sess, fn) {
    if(this.verbose > 90) {
      console.log("GoogleDataStore touch, sid: " + sid);
    }
    var store = this;
    if (!fn) fn = noop;

    try {
      var jsess = store.serializer.stringify(sess);
    }
    catch (er) {
      return fn(er);
    }


    const datastore = Datastore();
    const theKey = datastore.key([store.prefix, sid]);
    const entity = {
      key: theKey,
      data: [
        {
          name: 'sid',
          value: sid
        },
        {
          name: 'sess',
          value: jsess
        },
        {
          name: 'lastUsed',
          value: new Date().toJSON()
        }
      ]
    };

    datastore.update(entity)
      .then(() => {
        if(this.verbose > 40) {
          console.log("GoogleDataStore: touch key: " + JSON.stringify(theKey) + "-- saved successfully - for sid " + sid);
        }
        fn.apply(null, arguments);
      })
      .catch((err) => {
        console.log("GoogleDataStore: touch key: " + JSON.stringify(theKey) + "-- ERROR:: " + JSON.strigify(err));
        return fn(er);
    });
  };

  /**
   * Fetch all sessions' ids
   *
   * @param {Function} fn
   * @api public
   */

  GoogleDataStore.prototype.ids = function (fn) {
    if(this.verbose > 90) {
      console.log("GoogleDataStore : ids ");
    }
    fn(new Error("Not implemented"));
  };


  /**
   * Fetch all sessions
   *
   * @param {Function} fn
   * @api public
   */

  GoogleDataStore.prototype.all = function (fn) {
    if(this.verbose > 90) {
      console.log("GoogleDataStore : all ");
    }
    fn(new Error("Not implemented"));
  };

  return GoogleDataStore;
};