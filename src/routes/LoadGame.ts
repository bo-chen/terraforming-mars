import * as http from 'http';
import {Database} from '../database/Database';
import {GameLoader} from '../database/GameLoader';
import {Server} from '../models/ServerModel';
import {Handler} from './Handler';
import {IContext} from './IHandler';

export class LoadGame extends Handler {
  public static readonly INSTANCE = new LoadGame();
  private constructor() {
    super();
  }

  public put(req: http.IncomingMessage, res: http.ServerResponse, ctx: IContext): void {
    let body = '';
    req.on('data', function(data) {
      body += data.toString();
    });
    req.once('end', () => {
      try {
        const gameReq = JSON.parse(body);

        const game_id = gameReq.game_id;
        // This should probably be behind some kind of verification that prevents just
        // anyone from rolling back a large number of steps.
        const rollbackCount = gameReq.rollbackCount;
        GameLoader.getInstance().getByGameId(game_id, null, true, (game) => {
          if (game === undefined) {
            console.warn(`unable to find ${game_id} in database`);
            ctx.route.notFound(req, res, 'game_id not found');
            return;
          }
          if (rollbackCount > 0) {
            Database.getInstance().deleteGameNbrSaves(game_id, game.saveId!, rollbackCount);
            // TODO(bo-chen) This won't work properly without a callback through deleteGameNbrSaves.
            GameLoader.getInstance().getByGameId(game_id, game.parentSaveId!, true, (restoredGame) => {
              ctx.route.writeJson(res, Server.getGameModel(restoredGame!));
            });
          } else {
            ctx.route.writeJson(res, Server.getGameModel(game));
          }
        });
      } catch (error) {
        ctx.route.internalServerError(req, res, error);
      }
    });
  }
}
