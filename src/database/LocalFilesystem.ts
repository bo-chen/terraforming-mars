import {DbLoadCallback, IDatabase, IGameData} from './IDatabase';
import {Game, GameId, GameOptions, SaveId, Score} from '../Game';
import {SerializedGame} from '../SerializedGame';

import {Dirent} from 'fs';

const path = require('path');
const fs = require('fs');
const dbFolder = path.resolve(__dirname, '../../../db/files');
const historyFolder = path.resolve(dbFolder, 'history');
const startFolder = path.resolve(dbFolder, 'start');

export class Localfilesystem implements IDatabase {
  constructor() {
    console.log(`Starting local database at ${dbFolder}`);
    if (!fs.existsSync(dbFolder)) {
      fs.mkdirSync(dbFolder);
    }
    if (!fs.existsSync(historyFolder)) {
      fs.mkdirSync(historyFolder);
    }
    if (!fs.existsSync(startFolder)) {
      fs.mkdirSync(startFolder);
    }
  }

  _filename(gameId: GameId): string {
    return path.resolve(dbFolder, `game-${gameId}.json`);
  }

  _historyFilename(gameId: GameId, saveId: SaveId) {
    const saveIdString = saveId.toString().padStart(5, '0');
    return path.resolve(historyFolder, `game-${gameId}-${saveIdString}.json`);
  }

  _startFilename(gameId: GameId): string {
    return path.resolve(startFolder, `game-${gameId}.json`);
  }

  saveGame(game: Game, newSaveId: SaveId): void {
    // Start of a game if it's never been saved before
    const start = (game.saveId === undefined);

    // Set new save_id before saving
    game.parentSaveId = game.saveId;
    game.saveId = newSaveId;

    console.log(`saving ${game.id} at position ${newSaveId}`);
    this.saveSerializedGame(game.serialize(), start);
  }

  saveSerializedGame(serializedGame: SerializedGame, saveStart : boolean): void {
    const text = JSON.stringify(serializedGame, null, 2);
    if (saveStart) {
      fs.writeFileSync(this._startFilename(serializedGame.id), text);
    }
    fs.writeFileSync(this._filename(serializedGame.id), text);
    fs.writeFileSync(this._historyFilename(serializedGame.id, serializedGame.saveId!), text);
  }

  getGame(game_id: GameId, cb: (err: Error | undefined, game?: SerializedGame) => void): void {
    try {
      console.log(`Loading ${game_id}`);
      const text = fs.readFileSync(this._filename(game_id));
      const serializedGame = JSON.parse(text);
      cb(undefined, serializedGame);
    } catch (err) {
      cb(err, undefined);
    }
  }

  getGameVersion(_game_id: GameId, _save_id: SaveId, _cb: DbLoadCallback<SerializedGame>): void {
    throw new Error('Not implemented');
  }

  getClonableGames(cb: (err: Error | undefined, allGames: Array<IGameData>) => void) {
    this.getGames((err, gameIds) => {
      const filtered = gameIds.filter((gameId) => fs.existsSync(this._startFilename(gameId)));
      const gameData = filtered.map((gameId) => {
        const text = fs.readFileSync(this._startFilename(gameId));
        const serializedGame = JSON.parse(text) as SerializedGame;
        return {gameId: gameId, playerCount: serializedGame.players.length};
      });
      cb(err, gameData);
    });
  }

  loadCloneableGame(game_id: GameId, cb: DbLoadCallback<SerializedGame>) {
    try {
      console.log(`Loading ${game_id} at save point 0`);
      const text = fs.readFileSync(this._startFilename(game_id));
      const serializedGame = JSON.parse(text);
      cb(undefined, serializedGame);
    } catch (err) {
      cb(err, undefined);
    }
  }

  getGames(cb: (err: Error | undefined, allGames: Array<GameId>) => void) {
    const gameIds: Array<GameId> = [];

    // TODO(kberg): use readdir since this is expected to be async anyway.
    fs.readdirSync(dbFolder, {withFileTypes: true}).forEach((dirent: Dirent) => {
      if (!dirent.isFile()) {
        return;
      }
      const re = /game-(.*).json/;
      const result = dirent.name.match(re);
      if (result === null) {
        return;
      }
      gameIds.push(result[1]);
    });
    cb(undefined, gameIds);
  }

  saveGameResults(_game_id: GameId, _players: number, _generations: number, _gameOptions: GameOptions, _scores: Array<Score>): void {
    // Not implemented
  }

  cleanSaves(_game_id: GameId, _save_id: SaveId): void {
    // Not implemented here.
  }

  purgeUnfinishedGames(): void {
    // Not implemented.
  }

  deleteGameNbrSaves(_game_id: GameId, _fromSaveId : SaveId, _rollbackCount: number): void {
    throw new Error('Rollback not yet implemented');
  }
}
