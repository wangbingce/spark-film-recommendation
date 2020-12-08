import pandas as pd
import jieba
import jieba.analyse
import re
from collections import Counter

def make_key_words():
    data = pd.read_csv('./data/movies_detail.csv', names = ['id', 'name', 'intro'],usecols=[0,1,2])
    #data.head()
    content = ""
    r='[0-9\s+\.\!\/_,$%^*()?;；:-【】+\"\']+|[+——！，;：。？、 ~@#￥%……&*（）]+'
    for index, movie in data.iterrows():
        content = re.sub(r,"",str(movie['intro']))
        jieba.analyse.set_stop_words('./data/cn_stopwords.txt')
        '''
        word_large = jieba.lcut_for_search(content)
        key_word = " "
        word = []
        for character in word_large:
            if  len(character) > 1:
                word.append(character)
        
        counter = Counter(word)
        word_cnt = pd.DataFrame({"key":counter.keys(),"value":counter.values()})
        word_cnt = word_cnt.sort_values(by='value',ascending = False)
        if len(word_cnt)>5:
            key_word = [word_cnt.iloc[0,0],word_cnt.iloc[1,0],word_cnt.iloc[2,0],word_cnt.iloc[3,0],word_cnt.iloc[4,0]]
            movie['intro'] = key_word
        '''
        key_word = []
        key_word = jieba.analyse.extract_tags(content, topK=5, withWeight=False, allowPOS = ('ns','n'))
        data.at[index,'intro'] = key_word
    #data.head()
    return data

def concat_data(data):
    rating_data = pd.read_csv('./data/ratings.csv', names = ['user_id', 'movie_id', 'rating'], usecols=[0,1,2])
    rating_data['movie_name'] = None
    rating_data['movie_intro'] = None
    #rating_data.head()
    for index, user in rating_data.iterrows():
        rating_data.at[index , 'movie_id'] = (int(user['movie_id'])) % 10000
        user_movie = data.iloc[((int(user['movie_id'])-1) % 10000)]
        name = user_movie.loc['name']
        intro = user_movie.loc['intro']
        rating_data.at[index , 'movie_name'] = name
        rating_data.at[index , 'movie_intro'] = intro
    return rating_data

if __name__ == "__main__":
    key_word_data = make_key_words()
    rating_data = concat_data(key_word_data)
    key_word_data.to_csv('./data/key_word_data.csv', encoding = 'utf_8_sig' ,header = 0, index = 0, sep = ';')
    rating_data.to_csv( './data/rating_data.csv', encoding = 'utf_8_sig' ,header = 0, index = 0, sep = ';')
