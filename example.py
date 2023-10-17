import re
import requests


def chess(player:str, year:int, month:int):
    """
    Fetches results of chess games played on https://wwww.chess.com by the player with the given
    user name in the given month and tear.

    Parameters
    ----------
        player: str
        year: int
        month: int

    Yield
    -----
        dict

    Example
    -------
        >>> results = fetch_chess_com_results('magnuscarlsen', 2023, 10)
        >>> next(results)
            {
                'Moves': ['1. e4', '1... c5', '2. Nf3', '2... d6', ..., '36. Rc8+', '36... Qd8', '37. Rxd8#'],
                'TimeControl': '180+1',
                'Color': 'Black',
                'Outcome': 'Lose',
                'Rating': 2524,
                'OpponentRating': 3278
            }
    """    
    games = requests.get(
        f'https://api.chess.com/pub/player/{player}/games/{str(year)}/{str(month).rjust(2, "0")}',
        headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'}
    ).json()['games']
    moves_pattern = re.compile(r'[0-9]+[\.]+ [0-9A-Za-z-#+=]+')
    for game in games:
        color = 'white' if game['white']['username'] == player else 'black'
        yield dict(
            Moves=moves_pattern.findall(game['pgn'].splitlines()[-1]),
            TimeControl=game['time_control'],
            Color=color.title(),
            Outcome={'abandoned': 'Lose', 'win': 'Win', 'resigned': 'Lose', 'checkmated': 'Lose', 'stalemate': 'Draw', 'timeout': 'Lose', 'timevsinsufficient': 'Draw', 'agreed': 'Draw', 'repetition': 'Draw', 'insufficient': 'Draw'}[game[color]['result']],
            Rating=game[color]['rating'],
            Opponent=game['white' if color == 'black' else 'black']['username'],
            OpponentRating=game['white' if color == 'black' else 'black']['rating']
        )