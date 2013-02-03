
var should = require('should')
    , Prana = require('../lib/prana');

describe('prana', function() {

  var prana = Prana();

  beforeEach(function() {
    prana.start();
  });

  afterEach(function() {
    prana.stop();
  });

  describe('start', function() {
    it('should be running the service and continue polling without error', function(done) {
      should.exist(prana.timer);
      done();
    });
  });

  describe('stop', function() {
    it('should stop the service', function(done) {
      prana.stop();
      should.not.exist(prana.timer);
      done();
    });
  })

});
