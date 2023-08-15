import vk_api
import json
import re
import pandas as pd
import time
from config import TOKEN

session = vk_api.VkApi(token=TOKEN)
vk = session.get_api()
to_date = 1677618000
from_date = 1519851600

entertainments = [
    'забав', 'весел', 'смех', 'смеш', 'смея', 'смее', 'развлек', 'развлеч', 'увлеч', 'увлек',
    'фильм', 'музык', 'песн', 'выступл', 'юмор', 'вдохнов', 'поздрав', 'идея', 'идеи', 'плейлист',
    'подборк', 'делимся', 'делит', 'делил', 'видео', 'шутк', 'анекдот', 'забав', 'ожид', 'предвкуш',
    'наслажд', 'приятн', 'неотраз', 'праздн', 'фестив', 'интерес', 'завораж', 'зрелищ', 'отдых', 'отдохн',
    'релакс', 'расслаб', 'трепет', 'волнующ', 'взволн', 'соверш', 'сенсац', 'потряс', 'нов', 'прекрас', 'волшеб',
    'рекоменд', 'подборк', 'необходим'
]
trends = [
    'мем', 'вайб', 'донат', 'жиз', 'краш', 'кринж', 'крип', 'мерч',
    'олд', 'рил', 'рофл', 'стрим', 'топ', 'хайп', 'чил', 'шип', 'абьюз',
    'анпакинг', 'баг', 'байт', 'бомб', 'гон', 'душн', 'запил', 'зумер', 'лампов',
    'рандом', 'солд-аут', 'солдаут', 'соулмейт', 'токс', 'факап', 'фейк', 'фейспалм',
    'шейм', 'юзать', 'юзер', 'нейросет', 'вишлист', 'палит', 'современ', 'крут', 'креатив',
    'наряд', 'мод', 'знаменит', 'фешн', 'гламур', 'влиятельн', 'роскош', 'люкс', 'тренд',
    'популяр', 'известн', 'стил', 'топ'
]
interactions = [
    'акц', 'распрод', 'опрос', 'поделис', 'поделит', 'расскажи', 'ставь', 'делись', 'делитесь',
    'переходи', 'объявл', 'представл', 'улучш', 'требуется', 'сравн', 'спеш', 'чудесн', 'магич',
    'предлож', 'стань', 'зарегистрируй', 'перейди', 'розыгрыш', 'дарим', 'получи', 'отгадай', 'угадай',
    'найди', 'пиши', 'оцени', 'как тебе', 'как вам', 'неожид', 'сейчас', 'подпиши', 'вступай', 'нажми',
    'жми', 'репост', 'лайк', 'ставь', 'поставь', 'как ты', 'как вы', 'понравилось', 'задавай', 'решим', 'напишем',
    'расскажем', 'сделаем', 'задай', 'позвони', 'оставь', 'конкурс', 'игра', 'выиграй'
]
customs = [
    'ты', 'вы', 'здравств', 'добрый день', 'добрый вечер', 'доброе утро', 'помочь', 'обращай',
    'поможем', 'уточни', 'разберем', 'разобр', 'спасибо', 'благодар', 'приятно', 'рад', 'на связи', 'извини',
    'прости', 'расскажи', 'к сожалению', 'пожелания', 'обязательно', 'приносим', 'пожалуйста', 'стараемся',
    'для тебя', 'для вас', 'не переживай', 'у тебя', 'у вас', 'жаль', 'будем', 'обратить'
]

companies = [
    'citilink_ru',
    'eldorado_stores',
    'mvideo',
    'svyaznoy',
    're_store',
    'biggeekru',

    'befree_fashion',
    'loverepublic_official',
    'ostin',
    'gloria.jeans',
    '12storeezshop',
    'stockmann.russia',
    'dlt'

    'letoile_official',
    'goldapple_ru',
    'rivegauche',
    'ulybka_radugi',
    'podrygkashop',

    'autoru_news',
    'bibiauto49',
    'drom',
    'rolfcompany',
    'autorus1',
]


# возвращает посты сообщества с заданными параметрами
def get_posts(domain, offset=0):

    posts = session.method(
        'wall.get',
        {
            'domain': domain,
            'offset': offset,
            'count': 100,
            'filter': 'owner',
        }
    )

    return posts['items']


# возвращает комментарии с заданными параметрами
def get_comments(owner_id, post_id, offset=0, comment_id=0, count=100):
    try:
        comments = session.method(
            'wall.getComments',
            {
                'owner_id': owner_id,
                'post_id': post_id,
                'offset': offset,
                'count': count,
                'comment_id': comment_id,
                'thread_items_count': 10,
            }
        )
    except vk_api.exceptions.ApiError as error_msg:
        if error_msg.code == 212:
            return ['not_allowed'], -1, -1

    count = comments['count']
    current_level_count = comments['current_level_count']

    return comments, count, current_level_count


# возвращает информацию о сообществе
def get_members(group_id):
    members = session.method(
        'groups.getMembers',
        {
            'group_id': group_id,
        }
    )
    return members['count']


# удаляет посты, которые выходят за заданный срок
def fix_data(posts):
    fixed = []

    for post in posts:

        date = post['date']
        if date > to_date:
            continue

        elif date < from_date:
            return fixed

        fixed.append(post)
    return fixed


# собирает посты за указанный период
def collect_posts(company, to_date, from_date):
    date = to_date
    posts = []
    offset = 0

    while date > from_date:
        posts += get_posts(company, offset)

        offset += 100
        date = posts[-1]['date']
        print(offset)

    fixed_posts = fix_data(posts)

    return fixed_posts


# собирает комментарии постов за указанный период
def collect_comments(posts, to_date, from_date):
    comments = []
    i = 0

    for post in posts:
        all_comments = []
        date = post['date']

        if date > to_date:
            continue

        elif date < from_date:
            return comments

        owner_id = post['from_id']
        post_id = post['id']

        current_level_count, offset = 1, 0

        # собираем все комментарии к текущему посту
        while current_level_count - offset > 0:
            post_comments, count, current_level_count = get_comments(owner_id, post_id, offset)
            all_comments += post_comments['items']
            offset += 100

        comments.append(all_comments)
        i += 1
        print(f'{i} -- posts done')

    return comments


# собирает комментарии к комментариям
def collect_threads(all_comments, all_posts):
    all_threads = []
    i = 0

    for post in all_posts:
        post_threads = []
        post_comments = all_comments[i]

        count = post['comments']['count']
        current_level = len(post_comments)

        delta = count - current_level

        if count != current_level and count != 0:
            for comment in post_comments:
                thread_amount = comment['thread']['count']

                if delta == 0:
                    break

                if 0 < thread_amount <= 10:
                    delta -= thread_amount
                    post_threads += comment['thread']['items']

                elif thread_amount > 10 and not comment.get('deleted'):
                    delta -= thread_amount
                    comment_id, post_id, owner_id = comment['id'], comment['post_id'], comment['owner_id']
                    thread_count, thread_offset = 1, 0

                    while thread_count - thread_offset > 0:
                        comment_thread, thread_count, _ = get_comments(owner_id, post_id, thread_offset, comment_id)
                        thread_offset += 100
                        post_threads += comment_thread['items']
                        time.sleep(2)

        all_threads.append(post_threads)

        i += 1
        print(f'{i} -- posts done')

    return all_threads


# создает таблицу для импорта данных в Excel
def create_dataframe(posts, comments, threads):
    i = 0
    try:
        members = get_members(posts[0]['owner_id']*(-1))
    except vk_api.exceptions.ApiError as error_msg:
        if error_msg.code == 15:
            members = 477952
            print('exception')

    dataframe = {
        'text': [],
        'date': [],
        'comments': [],
        'comments_community': [],
        'entertainment': [],
        'interaction': [],
        'trend': [],
        'custom': [],
        'words': [],
        'attachments': [],
        'likes': [],
        'views': [],
        'reposts': [],
        'ER': []
    }

    for post in posts:
        all_comments = comments[i] + threads[i]
        text = post['text']
        owner_id = post['owner_id']
        likes = post['likes']['count']
        views = post['views']['count']
        reposts = post['reposts']['count']
        entertainment, trend, interaction, custom = [], [], [], []

        comments_amount = post['comments']['count']
        community_comments = []
        comments_community_amount = 0

        if comments_amount > 0:
            for comment in all_comments:
                if comment == 'not_allowed':
                    continue

                if comment['from_id'] == owner_id:
                    print('tut')
                    community_comments.append(comment['text'])
                    comments_community_amount += 1

        for word in entertainments:
            entertainment += re.findall(re.compile(f'{word}'), text)

        for word in trends:
            trend += re.findall(re.compile(f'{word}'), text)

        for word in interactions:
            interaction += re.findall(re.compile(f'{word}'), text)

        for word in customs:
            for comment in community_comments:
                custom += re.findall(re.compile(f'{word}'), comment)

        all_words = entertainment + trend + interaction + custom

        words = []
        for word in all_words:
            if word not in words:
                words.append(word)

        words = ', '.join(words)

        dataframe['text'] += [text]
        dataframe['date'] += [str(pd.to_datetime(post['date'], unit='s').date())]
        dataframe['comments'] += [comments_amount]
        dataframe['comments_community'] += [comments_community_amount]
        dataframe['entertainment'] += [len(entertainment)]
        dataframe['interaction'] += [len(interaction)]
        dataframe['trend'] += [len(trend)]
        dataframe['custom'] += [len(custom)]
        dataframe['words'] += [words]
        dataframe['likes'] += [likes]
        dataframe['views'] += [views]
        dataframe['reposts'] += [reposts]
        dataframe['ER'] += [((likes+reposts+comments_amount)/members)*100]

        try:
            dataframe['attachments'].append([attachment['type'] for attachment in post['attachments']])
        except IndexError:
            dataframe['attachments'] += ['None']

        i += 1

    return dataframe


for company in companies:

    print(f'Сбор постов сообщества {company}')
    posts = collect_posts(company, to_date, from_date)
    with open(f'./posts/{company}.json', 'w') as f:
        json.dump(posts, f)
    print(f'Завершен сбор постов сообщества {company}, всего постов: {len(posts)}')

    print(f'Сбор комментариев сообщества {company}')
    comments = collect_comments(posts, to_date, from_date)
    with open(f'./comments/{company}.json', 'w') as f:
        json.dump(comments, f)
    print(f'Завершен сбор комментариев сообщества {company})

    print(f'Сбор тредов комментариев к постам сообщества {company}')
    threads = collect_threads(comments, posts)
    with open(f'./threads/{company}.json', 'w') as f:
        json.dump(threads, f)
    print(f'Завершен сбор тредов комментариев к постам сообщества {company}')


writer = pd.ExcelWriter('ulybka_radugi.xlsx', engine='xlsxwriter')


for company in companies:

    with open(f'./posts/{company}.json') as f:
        posts = json.load(f)

    with open(f'./comments/{company}.json') as f:
        comments = json.load(f)

    with open(f'./threads/{company}.json') as f:
        threads = json.load(f)

    dataframe = create_dataframe(posts, comments, threads)

    with open(f'./dataframes/{company}.json', 'w') as f:
        json.dump(dataframe, f)

    table = pd.DataFrame(dataframe)

    table.to_excel(writer, sheet_name=company, index=False)

    print(f'TABLE SUCCESSFULLY CREATED -- {company}')

writer.save()
