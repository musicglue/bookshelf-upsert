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
        console.log(JSON.stringify(this, null, 4));
        throw new Error('Cannot upsert without uniqueness constraints');
      }

      function buildConflictors(fields, conflictors) {
        return conflictors.map(function(conflictor) {
          const alternates = _.reject(fields, function(field) {
            return field === conflictor
          });

          const updateStatements = _.compact(fields.map(function(field) {
            if (field === conflictor) { return null; }
            return `${field}=EXCLUDED.${field}`;
          })).join(', ');

          const returnStatement = (returning.length)
            ? `RETURNING (${returning.join(', ')})`
            : '';

          const fragment = Bookshelf.knex
            .raw(`DO UPDATE SET ${updateStatements} ${returnStatement}`)
            .toString();

          if (alternates.length) {
            return `ON CONFLICT (${conflictor}) ${fragment}`;
          }

          return 'ON CONFLICT DO NOTHING RETURNING id';
        }).join(', ');
      }

      const payload = this.attributes;
      const fields = Object.keys(payload);

      const upsertStatement = `
      INSERT INTO ${this.tableName} (${fields.join(', ')})
      VALUES (${fields.map(function(field) { return '?' }).join(', ')})
      ${buildConflictors(fields, conflictors)}
      `;

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
