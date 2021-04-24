import * as http from 'http';
import {Game} from '../Game';
import {Handler} from './Handler';
import {IContext} from './IHandler';
import {Server} from '../models/ServerModel';

export class ApiGame extends Handler {
  public static readonly INSTANCE = new ApiGame();
  private constructor() {
    super();
  }

  public get(req: http.IncomingMessage, res: http.ServerResponse, ctx: IContext): void {
    const gameId = ctx.url.searchParams.get('id');
    const saveId = ctx.url.searchParams.get('save-id');
    const shuffleDecks = ctx.url.searchParams.get('shuffle-decks');
    if (!gameId) {
      ctx.route.notFound(req, res, 'id parameter missing');
      return;
    }

    ctx.gameLoader.getByGameId(gameId, saveId, false, (game: Game | undefined) => {
      if (game === undefined) {
        ctx.route.notFound(req, res, 'game not found');
        return;
      }
      if (shuffleDecks) {
        game.dealer.shuffleDeck();
      }
      const model = Server.getGameModel(game);
      ctx.route.writeJson(res, model);
    });
  }
}
