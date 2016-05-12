const _ = require('lodash');

function replaceErrors(key, value) {
  if (_.isError(value)) {
    return value.message;
  }
  return value;
}

module.exports = {
  replaceErrors
};
