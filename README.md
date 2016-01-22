# Interface
API written by tornado

## Four interface

ip: 115.28.202.224
port: 8000

### 1. http://ip:port/get_now_events/n_timestamp
```
  parameter:
    n_timestamp: int   // 1434804000
  return:
	{
		'1': {
		'_id: UUID,
		'timestamp': str,
		'tweets_id': [],
		'burst_words': {word_1: num_n, ..., word_n: num_n},
		'burst_tweets_count': int,
		'sum_tweets_count': int,
		'parent_id': UUID
		},
		...
		'n': {}
	  }
```
  
### 2. http://ip:port/get_pre_events/s_timestamp/e_timestamp
```
parameter:
    s_timestamp: int,  // 1434798000
    e_timestamp: int  // 1434804600
  return:
  {
    '1': {
      '_id: UUID,
      'timestamp': str,
      'tweets_id': [],
      'burst_words': {word_1: num_n, ..., word_n: num_n},
      'burst_tweets_count': int,
      'sum_tweets_count': int,
      'parent_id': UUID
    },
    ...
    'n': {
    }
  }
```

### 3. http://ip:port/get_track_events/parent_id
```
  paramter:
    parent_id: UUID  // 56a1f2b9691bac57fb8c7ccf
  return:
  {
    '_id': UUID,
    'timestamp': str,
    'burst_events_objectid': [UUID, ..., UUID],
    'burst_words': {word_1: num_n, ..., word_n: num_n}
  }
```

### 4. http://ip:port/get_single_event/_id
```
  parameter: 
    _id: UUID  // 56a1f2b9691bac57fb8c7cce
  return:
  {
      '_id: UUID,
      'timestamp': str,
      'tweets_id': [],
      'burst_words': {word_1: num_n, ..., word_n: num_n},
      'burst_tweets_count': int,
      'sum_tweets_count': int,
      'parent_id': UUID
  }
``` 
 
### 5. http://ip:port/get_single_event_detail/_id
```
  parameter: 
    _id: int   // 7899937
  return:
  {
    'id': _id,
    'username': str,
    'timestamp': str,
    'pageurl': str,
    'location': str,
    'is_v': str,
    'score': float,
    'content': str
  }
```

### 注意
timestamp为str时形如：2015-06-20 19:20， 
timestamp为int时是由上面形式的时间转化成Unix时间所得



  
  
  
  
  
  
