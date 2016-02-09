module.exports = function(Bookshelf) {
  'use strict';
  var _ = require('lodash'),
      Promise = require('bluebird'),
      proto = Bookshelf.Model.prototype;

  var Model = Bookshelf.Model.extend({
    upsertMetadata: {
      conflictors: ['id'],
      returning: ['id'],
    },

    upsert: Promise.method(function(transaction) {
      const conflictors = this.upsertMetadata.conflictors,
            model = this;

      if (!conflictors) {
        throw new Error('Cannot upsert without uniqueness constraints');
      }

      function buildConflictors(fields, conflictors) {
        return conflictors.map(function(conflictor) {
          const updateStatements = fields.map(function(field) {
            return `${field}=EXCLUDED.${field}`;
          }).join(', ');

          const fragment = `DO UPDATE SET ${updateStatements}`;

          return `ON CONFLICT (${conflictor}) ${fragment}`;
        }).join(', ');
      }

      const payload = this.attributes;
      const fields = Object.keys(payload);

      const upsertStatement = `
        INSERT INTO ${this.tableName} (${fields.join(', ')})
        VALUES (${_.times(fields.length, function() { return '?'; }).join(', ')})
        ${buildConflictors(fields, conflictors)}
        RETURNING *`;

      return transaction
        .raw(upsertStatement, fields.map(function(field) { return payload[field]; }))
        .then(function(response) {
          return model.set(response.rows[0]);
        });
    }),
  });
  Bookshelf.Model = Model;
}
