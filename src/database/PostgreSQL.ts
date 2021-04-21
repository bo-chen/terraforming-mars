import {DbLoadCallback, IDatabase} from './IDatabase';
import {Game, GameId, GameOptions, SaveId, Score} from '../Game';
import {IGameData} from './IDatabase';
import {SerializedGame} from '../SerializedGame';

import {Client, ClientConfig, QueryResult} from 'pg';

export class PostgreSQL implements IDatabase {
  private client: Client;

  constructor() {
    const config: ClientConfig = {
      connectionString: process.env.POSTGRES_HOST,
    };
    if (config.connectionString !== undefined && config.connectionString.startsWith('postgres')) {
      config.ssl = {
        // heroku uses self-signed certificates
        rejectUnauthorized: false,
      };
    }
    this.client = new Client(config);
    this.client.connect();
    // Don't set foreign key for current_save_id and first_save_id so it's easier to delete
    this.client.query(
      `CREATE TABLE IF NOT EXISTS games(
        game_id VARCHAR PRIMARY KEY, 
        players INTEGER, 
        first_save_id VARCHAR,
        current_save_id VARCHAR,
        status TEXT DEFAULT 'running', 
        created_time TIMESTAMP NOT NULL DEFAULT now()
      )`);
    this.client.query(
      `CREATE TABLE IF NOT EXISTS saves(
        save_id VARCHAR PRIMARY KEY, 
        game_id VARCHAR NOT NULL, 
        game JSONB NOT NULL, 
        created_time TIMESTAMP NOT NULL DEFAULT now(), 
        FOREIGN KEY(game_id) REFERENCES games(game_id)
      )`);
    this.client.query(
      `CREATE TABLE IF NOT EXISTS game_results(
        game_id VARCHAR PRIMARY KEY, 
        seed_game_id VARCHAR NOT NULL, 
        players INTEGER NOT NULL, 
        generations INTEGER NOT NULL, 
        game_options JSONB NOT NULL, 
        scores JSONB NOT NULL
      )`);
    this.client.query('CREATE INDEX IF NOT EXISTS games_created_time_index on games(created_time)', (err) => {
      if (err) {
        throw err;
      }
    });
  }

  getClonableGames(cb: (err: Error | undefined, allGames: Array<IGameData>) => void) {
    const allGames: Array<IGameData> = [];
    const sql = 'SELECT game_id, players FROM games ORDER BY game_id ASC';

    this.client.query(sql, (err, res) => {
      if (err) {
        console.error('PostgreSQL:getClonableGames', err);
        cb(err, []);
        return;
      }
      for (const row of res.rows) {
        const gameId: GameId = row.game_id;
        const playerCount: number = row.players;
        const gameData: IGameData = {
          gameId,
          playerCount,
        };
        allGames.push(gameData);
      }
      cb(undefined, allGames);
    });
  }

  getGames(cb: (err: Error | undefined, allGames: Array<GameId>) => void) {
    const allGames: Array<GameId> = [];
    const sql: string = `SELECT game_id FROM games WHERE status = 'running' ORDER BY created_time DESC`;
    this.client.query(sql, (err, res) => {
      if (err) {
        console.error('PostgreSQL:getGames', err);
        cb(err, []);
        return;
      }
      for (const row of res.rows) {
        allGames.push(row.game_id);
      }
      cb(undefined, allGames);
    });
  }

  loadCloneableGame(game_id: GameId, cb: DbLoadCallback<SerializedGame>) {
    // Retrieve first save from database
    const sql = `SELECT s.game 
      FROM games g
      INNER JOIN saves s ON s.save_id = g.first_save_id
      WHERE g.game_id = $1`;
    this.client.query(sql, [game_id], (err: Error | undefined, res) => {
      if (err) {
        console.error('PostgreSQL:restoreReferenceGame', err);
        return cb(err, undefined);
      }
      if (res.rows.length === 0) {
        return cb(new Error(`Game ${game_id} not found`), undefined);
      }
      try {
        const json = JSON.parse(res.rows[0].game);
        return cb(undefined, json);
      } catch (exception) {
        console.error(`Unable to restore game ${game_id}`, exception);
        cb(exception, undefined);
        return;
      }
    });
  }

  // TODO(bo-chen) cb should have type DbLoadCallback<SerializedGame>?
  getGame(game_id: GameId, cb: (err: Error | undefined, game?: SerializedGame) => void): void {
    // Retrieve last save from database
    const sql = `SELECT s.game 
      FROM games g
      INNER JOIN saves s ON s.save_id = g.current_save_id
      WHERE g.game_id = $1`;
    this.client.query(sql, [game_id], (err, res) => {
      if (err) {
        console.error('PostgreSQL:getGame', err);
        return cb(err);
      }
      if (res.rows.length === 0) {
        return cb(new Error('Game not found'));
      }
      cb(undefined, JSON.parse(res.rows[0].game));
    });
  }

  getGameVersion(game_id: GameId, save_id: SaveId, cb: DbLoadCallback<SerializedGame>): void {
    this.client.query('SELECT game FROM saves WHERE game_id = $1 AND save_id = $2', [game_id, save_id], (err: Error | null, res: QueryResult<any>) => {
      if (err) {
        console.error('PostgreSQL:getGameVersion', err);
        return cb(err, undefined);
      }
      cb(undefined, JSON.parse(res.rows[0].game));
    });
  }

  saveGameResults(game_id: GameId, players: number, generations: number, gameOptions: GameOptions, scores: Array<Score>): void {
    this.client.query('INSERT INTO game_results (game_id, seed_game_id, players, generations, game_options, scores) VALUES($1, $2, $3, $4, $5, $6)',
      [game_id, gameOptions.clonedGamedId, players, generations, gameOptions, JSON.stringify(scores)], (err) => {
        if (err) {
          console.error('PostgreSQL:saveGameResults', err);
          throw err;
        }
      });
  }

  cleanSaves(game_id: GameId, _save_id: SaveId): void {
    // DELETE all saves except initial and last one
    this.client.query('SELECT first_save_id, current_save_id FROM games WHERE game_id = $1', [game_id], (err: Error | null, res: QueryResult<any>) => {
      if (err) {
        console.error('PostgreSQL:cleanSaves', err);
        throw err;
      }
      if (res.rowCount === 0) {
        return console.warn(`Couldn't find game ${game_id} to cleanSaves`);
      }
      const row = res.rows[0];
      this.client.query('DELETE FROM saves WHERE game_id = $1 AND save_id != $2 AND save_id != $3', [game_id, row.current_save_id, row.first_save_id], (err2) => {
        if (err2) {
          console.error('PostgreSQL:cleanSaves2', err2);
          throw err2;
        }
      });
      // Flag game as finished
      this.client.query(`UPDATE games SET status = 'finished' WHERE game_id = $1`, [game_id], (err3) => {
        if (err3) {
          console.error('PostgreSQL:cleanSaves2', err3);
          throw err3;
        }
      });
    });
    this.purgeUnfinishedGames();
  }

  // Purge unfinished games older than MAX_GAME_DAYS days. If this environment variable is absent, it uses the default of 10 days.
  purgeUnfinishedGames(): void {
    const envDays = parseInt(process.env.MAX_GAME_DAYS || '');
    const days = Number.isInteger(envDays) ? envDays : 10;
    this.client.query(`SELECT game_id FROM games WHERE created_time < now() - interval '1 day' * $1 and status = 'running'`, [days], (err?: Error, res?: QueryResult<any>) => {
      if (err) {
        console.warn('PostgreSQL:purgeUnfinishedGames1', err.message);
        return;
      }
      if (res === undefined) {
        console.warn(`PostgreSQL:purgeUnfinishedGames2`);
        return;
      }

      if (res.rowCount > 0) {
        const placeholders : string = res.rows.map(() => '?').join(',');
        const gameIds : Array<SaveId> = res.rows.map((r) => r.game_id);
        this.client.query(`DELETE FROM saves WHERE game_id IN (${placeholders})`, gameIds, (err?: Error, res?: QueryResult<any>) => {
          if (err) {
            console.warn('PostgreSQL:purgeUnfinishedGames3', err.message);
            return;
          }
          if (res) {
            console.log(`Purged ${res.rowCount} saves`);
          }
        });
        this.client.query(`DELETE FROM games WHERE game_id IN (${placeholders})`, gameIds, (err?: Error, res?: QueryResult<any>) => {
          if (err) {
            console.warn('PostgreSQL:purgeUnfinishedGames4', err.message);
            return;
          }
          if (res) {
            console.log(`Purged ${res.rowCount} games`);
          }
        });
      }
    });
  }

  restoreGame(game_id: GameId, save_id: SaveId, cb: DbLoadCallback<Game>): void {
    // Retrieve last save from database
    this.client.query('SELECT game FROM saves WHERE game_id = $1 AND save_id = $2', [game_id, save_id], (err, res) => {
      if (err) {
        console.error('PostgreSQL:restoreGame', err);
        cb(err, undefined);
        return;
      }
      if (res.rows.length === 0) {
        console.error('PostgreSQL:restoreGame', `Game ${game_id} not found`);
        cb(err, undefined);
        return;
      }
      try {
        // Transform string to json
        const json = JSON.parse(res.rows[0].game);
        const game = Game.deserialize(json);
        cb(undefined, game);
      } catch (err) {
        cb(err, undefined);
      }
    });
  }

  saveGame(game: Game, newSaveId: SaveId): void {
    // The flow is a bit different for first saves -- where the game reference also needs to be created
    const firstSave = (game.saveId === undefined);

    // Set new save_id before saving to db
    game.parentSaveId = game.saveId;
    game.saveId = newSaveId;

    const gameJSON = game.toJSON();

    if (firstSave) {
      // Insert game before inserting save for first save.
      const sql = 'INSERT INTO games (game_id, current_save_id, first_save_id, players) VALUES ($1, $2, $2, $3)';
      this.client.query(sql, [game.id, newSaveId, game.getPlayers().length], (err) => {
        if (err) {
          console.error('PostgreSQL:saveGame1', err);
          return;
        }
        this.client.query('INSERT INTO saves (game_id, save_id, game) VALUES ($1, $2, $3)', [game.id, newSaveId, gameJSON], (err) => {
          if (err) {
            console.error('PostgreSQL:saveGame2', err);
            return;
          }
        });
      });
    } else {
      // For most saves, it's better to insert the save first before updating game.
      this.client.query('INSERT INTO saves (game_id, save_id, game) VALUES ($1, $2, $3)', [game.id, newSaveId, gameJSON], (err) => {
        if (err) {
          console.error('PostgreSQL:saveGame3', err);
          return;
        }
        this.client.query('UPDATE games SET current_save_id = $1 WHERE game_id = $2', [newSaveId, game.id], (err) => {
          if (err) {
            console.error('PostgreSQL:saveGame4', err);
            return;
          }
        });
      });
    }
  }

  deleteGameNbrSaves(game_id: GameId, fromSaveId : SaveId, rollbackCount: number): void {
    if (rollbackCount <= 0) {
      return;
    }
    this.client.query('SELECT game FROM games WHERE game_id = ? AND save_id = ?', [game_id, fromSaveId], (err, res) => {
      if (err) {
        console.error('PostgreSQL:saveGame', err);
        return;
      }
      if (res.rows.length === 0) {
        console.error('PostgreSQL:deleteGameNbrSaves', `Game ${game_id} not found`);
        return;
      }
      const json = JSON.parse(res.rows[0].game);
      const parent = json.parentSaveId ?? null;
      if (parent === null) {
        return console.warn(`Game ${game_id} could not be rolled back behind the root save ${fromSaveId}`);
      }
      this.client.query('DELETE FROM saves WHERE game_id = $1 AND save_id = $2', [game_id, fromSaveId], (err) => {
        if (err) {
          console.error('PostgreSQL:saveGame2', err);
          return;
        }
        if (rollbackCount > 1 && parent !== null) {
          this.deleteGameNbrSaves(game_id, parent as SaveId, rollbackCount - 1);
        } else {
          this.client.query('UPDATE games SET current_save_id = $1 WHERE game_id = $2', [parent, game_id], (err) => {
            if (err) {
              console.error('PostgreSQL:saveGame3', err);
              return;
            }
          });
        }
      });
    });
  }
}
