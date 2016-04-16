# Interface
API written by tornado

## Four interface

ip: 202.119.84.47
port: 8888

### 1. http://ip:port/get_detection_event_by_id/id
```
    parameter:
        _id: object_id // 56a1f2b9691bac57fb8c7ccf
    return:
    {
		'_id: object_id,
		'parent_id': object_id
		'timestamp': str,
		'tweets_id': [],
		'sentiment_pos': float,
		'sentiment_neu': float,
		'sentiment_neg': float,
		'burst_words': {word_1: num_n, ..., word_n: num_n},
		'burst_tweets_count': int,
		'sum_tweets_count': int,
    }
```

### 2. http://ip:port/get_detection_events_by_time/e_timestamp
```
    parameter:
        e_timestamp: int // 1434804600
    return:
    {
		'1': {
		'_id: object_id,
		'parent_id': object_id
		'timestamp': str,
		'tweets_id': [],
		'sentiment_pos': float,
		'sentiment_neu': float,
		'sentiment_neg': float,
		'burst_words': {word_1: num_n, ..., word_n: num_n},
		'burst_tweets_count': int,
		'sum_tweets_count': int,
		},
		...
		'n': {}
    }
```

### 3. http://ip:port/get_tracking_event_by_id/id
```
    parameter:
        id: object_id // 56a1f2b9691bac57fb8c7cce
    return:
    {
        '_id': object_id,
        'timestamp': str,
        'sentiment': float,
        'burst_events_objectid': [object_id, ..., object_id],
        'burst_words': {word_1: num_n, ..., word_n: num_n}
    }
```

### 4. http://ip:port/get_tracking_events_by_time/s_timestamp/e_timestamp
```
    parameter:
        s_timestamp: int, // 1434801600
        e_timestamp: int // 1434804600
    return:
    {
        '1': {
            '_id': object_id,
            'timestamp': str,
            'sentiment': float,
            'burst_events_objectid': [object_id, ..., object_id],
            'burst_words': {word_1: num_n, ..., word_n: num_n}
        },
        ...
        'n': {}
    }
```

### 5. http://ip:port/get_events_by_parentid/parent_id
```
  parameter:
    id: object_id //
  return:
  {
    '1': {
      '_id: object_id,
      'timestamp': str,
      'tweets_id': [],
      'sentiment': float,
      'burst_words': {word_1: num_n, ..., word_n: num_n},
      'burst_tweets_count': int,
      'sum_tweets_count': int,
      'parent_id': object_id
    },
    ...,
    'n': {
    }
  }
```

### 6. http://ip:port/get_single_tweet_by_id/id
```
    parameter:
        id: int   // 7899937
    return:
    {
        'id': id,
        'username': str,
        'timestamp': str,
        'pageurl': str,
        'location': str,
        'is_v': str,
        'score': float,
        'content': str,
        'pos': float,
        'neu': float,
        'neg': float
    }
```

### 7. http://ip:port/get_twwets_by_eventid/id
```
    parameter:
        id: object_id //
    return:
    {
        '1': {
            'id': id,
            'username': str,
            'timestamp': str,
            'pageurl': str,
            'location': str,
            'is_v': str,
            'score': float,
            'content': str,
            'pos': float,
            'neu': float,
            'neg': float
        },
        ...,
        'n': {
        }
    }
```

### 7. http://ip:port/get_twwets_by_trackid/id
```
    parameter:
        id: object_id //
    return:
    {
        'event1_object_id': {
            'id': id,
            'username': str,
            'timestamp': str,
            'pageurl': str,
            'location': str,
            'is_v': str,
            'score': float,
            'content': str,
            'pos': float,
            'neu': float,
            'neg': float
        },
        ...,
        'eventn_object_id': {
        }
    }
```

## 注意

1. timestamp为str时形如：2015-06-20 19:20，分钟数必须能被10整除，目前有效值为2015-06-20 19:00~2015-06-20 20:50；

2. timestamp为int时是由上面形式的时间转化成Unix时间所得

3. object_id 是Mongodb产生的UUID。
