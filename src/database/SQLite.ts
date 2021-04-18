import {DbLoadCallback, IDatabase} from './IDatabase';
import {Game, GameId, GameOptions, Score} from '../Game';
import {IGameData} from './IDatabase';
import {SerializedGame} from '../SerializedGame';

import sqlite3 = require('sqlite3');
const path = require('path');
const fs = require('fs');
const dbFolder = path.resolve(__dirname, '../../../db');
const dbPath = path.resolve(__dirname, '../../../db/game.db');

export class SQLite implements IDatabase {
  private db: sqlite3.Database;

  constructor() {
    // Create the table that will store every saves if not exists
    if (!fs.existsSync(dbFolder)) {
      fs.mkdirSync(dbFolder);
    }
    this.db = new sqlite3.Database(dbPath);
    // Don't set foreign key for current_save_id and first_save_id so it's easier to delete
    this.db.run(
      `CREATE TABLE IF NOT EXISTS games(
        game_id VARCHAR PRIMARY KEY, 
        players INTEGER, 
        first_save_id VARCHAR,
        current_save_id VARCHAR,
        status text DEFAULT 'running', 
        created_time TIMESTAMP DEFAULT (strftime('%s', 'now')),
      )`);
    this.db.run(
      `CREATE TABLE IF NOT EXISTS saves(
        save_id VARCHAR PRIMARY KEY, 
        game_id VARCHAR NOT NULL, 
        game TEXT NOT NULL, 
        created_time TIMESTAMP NOT NULL DEFAULT (strftime('%s', 'now'), 
        FOREIGN KEY(game_id) REFERENCES games(game_id)
      )`);
    this.db.run(
      `CREATE TABLE IF NOT EXISTS game_results(
        game_id VARCHAR PRIMARY KEY, 
        seed_game_id VARCHAR NOT NULL, 
        players INTEGER NOT NULL, 
        generations INTEGER NOT NULL, 
        game_options TEXT NOT NULL, 
        scores TEXT NOT NULL
      )`);
  }

  getClonableGames(cb: (err: Error | undefined, allGames: Array<IGameData>) => void) {
    const allGames: Array<IGameData> = [];
    const sql = `SELECT game_id, players FROM games ORDER BY game_id ASC`;

    this.db.all(sql, [], (err, rows) => {
      if (rows) {
        rows.forEach((row) => {
          const gameId: GameId = row.game_id;
          const playerCount: number = row.players;
          const gameData: IGameData = {
            gameId,
            playerCount,
          };
          allGames.push(gameData);
        });
        return cb(err ?? undefined, allGames);
      }
    });
  }

  getGames(cb: (err: Error | undefined, allGames: Array<GameId>) => void) {
    const allGames: Array<GameId> = [];
    const sql: string = `SELECT game_id FROM games WHERE status = 'running' ORDER BY created_at DESC`;
    this.db.all(sql, [], (err, rows) => {
      if (rows) {
        rows.forEach((row) => {
          allGames.push(row.game_id);
        });
        return cb(err ?? undefined, allGames);
      }
    });
  }

  loadCloneableGame(game_id: GameId, cb: DbLoadCallback<SerializedGame>) {
    // Retrieve first save from database
    const sql = `SELECT s.game 
      FROM games g
      INNER JOIN saves s ON s.save_id = g.first_save_id
      WHERE g.game_id = ?`;
    this.db.get(sql, [game_id], (err: Error | null, row: { game_id: GameId, game: any; }) => {
      if (row === undefined) {
        return cb(new Error('Game not found'), undefined);
      }

      try {
        const json = JSON.parse(row.game);
        return cb(err ?? undefined, json);
      } catch (exception) {
        console.error(`unable to load game ${game_id} at first save point`, exception);
        cb(exception, undefined);
        return;
      }
    });
  }

  saveGameResults(game_id: GameId, players: number, generations: number, gameOptions: GameOptions, scores: Array<Score>): void {
    this.db.run(
      'INSERT INTO game_results (game_id, seed_game_id, players, generations, game_options, scores) VALUES($1, $2, $3, $4, $5, $6)',
      [game_id, gameOptions.clonedGamedId, players, generations, JSON.stringify(gameOptions), JSON.stringify(scores)], (err) => {
        if (err) {
          console.error('SQLite:saveGameResults', err);
          throw err;
        }
      },
    );
  }

  getGame(game_id: GameId, cb: (err: Error | undefined, game?: SerializedGame) => void): void {
    // Retrieve last save from database
    const sql = `SELECT s.game 
      FROM games g
      INNER JOIN saves s ON s.save_id = g.current_save_id
      WHERE g.game_id = ?`;
    this.db.get(sql, [game_id], (err: Error | null, row: { game_id: GameId, game: any; }) => {
      if (err) {
        return cb(err ?? undefined);
      }
      cb(undefined, JSON.parse(row.game));
    });
  }

  getGameVersion(game_id: GameId, save_id: string, cb: DbLoadCallback<SerializedGame>): void {
    this.db.get('SELECT game FROM saves WHERE game_id = ? AND save_id = ?', [game_id, save_id], (err: Error | null, row: { game: any; }) => {
      if (err) {
        return cb(err ?? undefined, undefined);
      }
      cb(undefined, JSON.parse(row.game));
    });
  }

  cleanSaves(game_id: GameId, _save_id: string): void {
    // DELETE all saves except initial and last one
    this.db.get('SELECT first_save_id, current_save_id FROM games WHERE game_id = ?', [game_id], (err: Error | null, row: { first_save_id: any, current_save_id: any }) => {
      if (err) {
        return console.warn(err.message);
      }
      if (row === undefined) {
        return console.warn(`Couldn't find game ${game_id} to cleanSaves`);
      }
      this.db.run(`DELETE FROM saves WHERE game_id = ? AND save_id != ? AND save_id != ?`, [game_id, row.current_save_id, row.first_save_id], function(err: Error | null) {
        if (err) {
          return console.warn(err.message);
        }
      });
      // Flag game as finished
      this.db.run(`UPDATE games SET status = 'finished' WHERE game_id = ?`, [game_id], function(err: Error | null) {
        if (err) {
          return console.warn(err.message);
        }
      });
    });

    this.purgeUnfinishedGames();
  }

  purgeUnfinishedGames(): void {
    // Purge unfinished games older than MAX_GAME_DAYS days. If this .env variable is not present, unfinished games will not be purged.
    if (process.env.MAX_GAME_DAYS) {
      this.db.all(`SELECT game_id FROM games WHERE created_time < strftime('%s',date('now', '-? day')) and status = 'running'`, [process.env.MAX_GAME_DAYS], (err: Error | null, rows: Array<{ game_id : string }>) => {
        if (err) {
          return console.warn(err?.message);
        }
        if (rows.length > 0) {
          const placeholders : string = rows.map(() => '?').join(',');
          const gameIds : Array<string> = rows.map((r) => r.game_id);
          this.db.run(`DELETE FROM saves WHERE game_id IN (${placeholders})`, gameIds, (err: Error | null) => {
            if (err) {
              return console.warn(err?.message);
            }
            this.db.run(`DELETE FROM games WHERE game_id IN (${placeholders}) `, gameIds, function(err: Error | null) {
              if (err) {
                return console.warn(err.message);
              }
            });
          });
        }
      });
    }
  }

  restoreGame(game_id: GameId, save_id: string, cb: DbLoadCallback<Game>): void {
    this.db.get('SELECT game FROM saves WHERE game_id = ? AND save_id = ?', [game_id, save_id], (err: Error | null, row: { game: any; }) => {
      if (err) {
        console.error(err.message);
        cb(err, undefined);
        return;
      }
      try {
        const json = JSON.parse(row.game);
        const game = Game.deserialize(json);
        cb(undefined, game);
      } catch (err) {
        cb(err, undefined);
      }
    });
  }

  saveGame(game: Game): void {
    // Set new save_id before saving to db
    game.parentSaveId = game.saveId;
    game.saveId = this.generateRandomId('v');
    const save_id = game.saveId;

    const gameJSON = game.toJSON();

    // Upsert game before inserting save
    const sql = 'INSERT INTO games (game_id, current_save_id, first_save_id, players) VALUES (?, ?, ?, ?) ON CONFLICT (game_id) DO UPDATE SET current_game_id = ?';
    this.db.run(sql, [game.id, save_id, save_id, game.getPlayers().length, save_id], (err: Error | null) => {
      if (err) {
        console.error(err.message);
        return;
      }

      this.db.run('INSERT INTO saves (game_id, save_id, game) VALUES ($1, $2, $3)', [game.id, save_id, gameJSON], function(err: Error | null) {
        if (err) {
          console.error(err.message);
          return;
        }
      });
    });
  }

  deleteGameNbrSaves(game_id: GameId, fromSaveId : string, rollbackCount: number): void {
    if (rollbackCount <= 0) {
      return;
    }
    this.db.get('SELECT game FROM saves WHERE game_id = ? AND save_id = ?', [game_id, fromSaveId], (err: Error | null, row: { game: any; }) => {
      if (err) {
        return console.warn(err.message);
      }
      const json = JSON.parse(row.game);
      const parent = json.parentSaveId ?? null;
      if (parent === null) {
        return console.warn(`Game ${game_id} could not be rolled back behind the root save ${fromSaveId}`);
      }
      this.db.run('DELETE FROM saves WHERE game_id = ? AND save_id = ?', [game_id, fromSaveId], (err: Error | null) => {
        if (err) {
          return console.warn(err.message);
        }
        if (rollbackCount > 1 && parent !== null) {
          this.deleteGameNbrSaves(game_id, parent as string, rollbackCount - 1);
        } else {
          this.db.run('UPDATE games SET current_save_id = ? WHERE game_id = ?', [parent, game_id], function(err: Error | null) {
            if (err) {
              return console.warn(err.message);
            }
          });
        }
      });
    });
  }

  // TODO(Bo) Both this and the copy in GameHandler should probably be moved to Game.ts
  public generateRandomId(prefix: string): string {
    // 281474976710656 possible values.
    return prefix + Math.floor(Math.random() * Math.pow(16, 12)).toString(16);
  }
}
