# Interface
API written by tornado

## Four interface


1. http://ip:8000/get_now_events/n_timestamp
```
  parameter:
    n_timestamp: int
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
  
2. http://ip:8000/get_pre_events/s_timestamp/e_timestamp
```
	parameter:
    s_timestamp: int,
    e_timestamp: int
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

3. http://ip:8000/get_track_events/parent_id
```
  paramter:
    parent_id: UUID
  return:
  {
    '_id': UUID,
    'timestamp': str,
    'burst_events_objectid': [UUID, ..., UUID],
    'burst_words': {word_1: num_n, ..., word_n: num_n}
  }
```

4. http://ip:8000/get_single_event/_id
```
  parameter: 
    _id: UUID
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
 
5. http://ip:8000/get_single_event_detail/_id
```
  parameter: 
    _id: 7971517
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




  
  
  
  
  
  
