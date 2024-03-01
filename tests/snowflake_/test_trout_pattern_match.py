import pandas as pd
from src.snowflake_ import trout_pattern_match

def test_clean_data():
    test = pd.DataFrame(
    {
        'wrong' : ['11 mile', '11mile', 'deckers', 'cheesman canyon', 'cheeseman', 'dream stream', 'treys pond', 'resevoir', 'north fork south platte', 'spinney', 'Spinney mountain()'],
        'right' : ['eleven mile', 'eleven mile', 'south platte', 'south platte', 'cheesman', 'south platte', 'treys', '', 'south platte',  'spinney mountain', 'spinney mountain']
    }
    )

    trout_pattern_match._clean_data(test, cols=['wrong'])

    assert all(test.right == test.wrong_clean)