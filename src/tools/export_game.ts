import {Database} from '../database/Database';
import {Localfilesystem} from '../database/LocalFilesystem';
import {SerializedGame} from '../SerializedGame';
const args = process.argv.slice(2);
const gameId = args[0];

if (gameId === undefined) {
  throw new Error('missing game id');
}
if (process.env.LOCAL_FS_DB !== undefined) {
  throw new Error('Do not run exportGame on local filesystem. Just access the files themselves');
}

const db = Database.getInstance();
const localDb = new Localfilesystem();

// Recursively copy all versions from saveId to root
function copySaveId(gameId : string, saveId : string) {
  db.getGameVersion(gameId, saveId, (err, serialized) => {
    if (err) {
      console.error(err);
      return;
    }
    console.log(`Storing version ${saveId}`);
    if (serialized!.parentSaveId === undefined) {
      // At root
      localDb.saveSerializedGame(serialized!, true);
    } else {
      localDb.saveSerializedGame(serialized!, false);
      copySaveId(gameId, serialized!.parentSaveId);
    }
  });
}

console.log(`Loading game ${gameId}`);
db.getGame(gameId, (err: Error | undefined, game?: SerializedGame) => {
  if (err) {
    console.log(err);
    process.exit(1);
  }
  if (game === undefined) {
    console.log('Game is undefined');
    process.exit(1);
  }

  console.log(`Last version is ${game.saveId}`);
  // recurse and save all versions
  copySaveId(gameId, game.saveId!);
  // save current version again so it is saved as the "current" game
  localDb.saveSerializedGame(game!, false);
});

