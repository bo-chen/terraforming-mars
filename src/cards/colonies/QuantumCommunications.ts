import { IProjectCard } from "../IProjectCard";
import { Tags } from "../Tags";
import { CardType } from '../CardType';
import { Player } from "../../Player";
import { CardName } from '../../CardName';
import { Resources } from "../../Resources";
import { Game } from '../../Game';

export class QuantumCommunications implements IProjectCard {
    public cost: number = 8;
    public tags: Array<Tags> = [];
    public name: string = CardName.QUANTUM_COMMUNICATIONS;
    public cardType: CardType = CardType.AUTOMATED;

    public canPlay(player: Player): boolean {
        return player.getTagCount(Tags.SCIENCE) >= 4;
    }

    public play(player: Player, game: Game) {
      let coloniesCount: number = 0;
      game.colonies.forEach(colony => { 
        coloniesCount += colony.colonies.length;
      });  
      player.setProduction(Resources.MEGACREDITS, coloniesCount);  
      return undefined;
    }

    public getVictoryPoints() {
        return 1;
    }
}