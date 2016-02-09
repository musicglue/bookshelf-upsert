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
            returning = this.upsertMetadata.returning,
            model = this;

      if (!conflictors) {
        throw new Error('Cannot upsert without uniqueness constraints');
      }

      function buildConflictors(fields, conflictors) {
        return conflictors.map(function(conflictor) {
          const alternates = _.reject(fields, function(field) {
            return field === conflictor;
          });

          const updateStatements = alternates.map(function(field) {
            return `${field}=EXCLUDED.${field}`;
          }).join(', ');

          const fragment = `DO UPDATE SET ${updateStatements}`;

          return (alternates.length) ?
            `ON CONFLICT (${conflictor}) ${fragment}` :
            'ON CONFLICT DO NOTHING RETURNING id';
        }).join(', ');
      }

      const payload = this.attributes;
      const fields = Object.keys(payload);

      const returnStatement = returning.length ? `RETURNING (${returning.join(', ')})` : '';
      const upsertStatement = `
        INSERT INTO ${this.tableName} (${fields.join(', ')})
        VALUES (${_.times(fields.length, function() { return '?'; }).join(', ')})
        ${buildConflictors(fields, conflictors)}
        ${returnStatement}`;

      return transaction
        .raw(upsertStatement, fields.map(function(field) { return payload[field]; }))
        .then(function(response) {
          model.set(response.rows[0]);
          return model;
        });
    }),
  });
  Bookshelf.Model = Model;
}
