'''Redis command handlers, data store, and server configuration.'''

import sys
import secrets
import asyncio
from time import time
from typing import NamedTuple

# Map keys to StoreEntry objects representing the Redis data store.
STORE = {}

# Map keys to lists of asyncio events for blocking commands (e.g., BLPOP, XREAD).
WAITERS = {}

# Contains writers for connections where PSYNC was received.
REPLICA_WRITERS = []

# Commands that modify STORE and must be propagated to replicas.
WRITE_COMMANDS = ['SET', 'RPUSH', 'LPUSH', 'LPOP', 'BLPOP', 'XADD', 'INCR']

# Server configuration and replication metadata.
SERVER_INFO = {
    'role': 'master',  # The server's replication role ('master' or 'slave').
    'master_replid': secrets.token_hex(20),  # Unique replication ID used to identify the master.
    'master_repl_offset': '0',  # Number of bytes the master has propagated to replicas.
}


class StoreEntry(NamedTuple):
    '''Represents a value stored in the Redis data store.

    Attributes:
        value: The stored value, which can be a string, a list of strings,
               or a list of stream entries.
        expiry_time: The Unix timestamp at which the entry expires,
                     or None if the entry has no expiry.
        redis_type: The Redis type of the stored value (e.g. 'string',
                    'list', 'stream').
    '''
    value: str | list[str] | list[tuple[str, dict[str, str]]]
    expiry_time: float | None
    redis_type: str


def run_ping(args: list[str]) -> bytes:
    '''Test the connection between a client and the server.
    
    Returns:
        A +PONG response to indicate a successful connection.
    '''
    return b'+PONG\r\n'


def run_echo(args: list[str]) -> bytes:
    '''Return the provided string back to the client.
    
    Args:
        args: The list of arguments where args[0] is the string to echo. 

    Returns: 
        The input string encoded as a RESP bulk string.
    '''
    return f'${len(args[0])}\r\n{args[0]}\r\n'.encode()


def run_type(args: list[str]) -> bytes:
    '''Look up the Redis type of the value stored at the given key.
    
    Args:
        args: The list of arguments where args[0] is the key to look up.

    Returns:
        The Redis type as a RESP simple string, or +none if the key doesn't exist.
    '''
    key = args[0]
    store_entry = STORE.get(key)

    # If the key doesn't exist
    if store_entry is None:
        return b'+none\r\n'
    
    return f'+{store_entry.redis_type}\r\n'.encode()


def run_set(args: list[str]) -> bytes:
    '''Store a key-value pair in the data store with an optional expiry time.

    Args:
        args: The list of arguments where args[0] is the key, args[1] is the value,
              and optional PX or EX arguments specify the expiry in milliseconds or seconds.

    Returns:
        +OK on success.
    '''
    key = args[0]
    expiry_time = None

    if 'PX' in args: 
        expiry_time = time() + (int(args[3]) / 1000)
    elif 'EX' in args: 
        expiry_time = time() + int(args[3])

    STORE[key] = StoreEntry(value=args[1], expiry_time=expiry_time, redis_type='string')

    return b'+OK\r\n'


def run_get(args: list[str]) -> bytes:
    '''Retrieve the value stored at the given key.

    Args:
        args: The list of arguments where args[0] is the key to look up.

    Returns:
        The value as a RESP bulk string, or a null bulk string if the key
        doesn't exist or has expired.
    '''
    key = args[0]
    store_entry = STORE.get(key)

    if store_entry is None: 
        return b'$-1\r\n'
    elif store_entry.expiry_time is not None and time() > store_entry.expiry_time:  # If there's an expiry time and the current time exceeds it, the key is expired. 
        return b'$-1\r\n'
    
    return f'${len(store_entry.value)}\r\n{store_entry.value}\r\n'.encode()


def run_rpush(args: list[str]) -> bytes:
    '''Append one or more elements to the tail of a list.

    Args:
        args: The list of arguments where args[0] is the key and args[1:] are
              the elements to append.

    Returns:
        The length of the list after the push as a RESP integer.
    '''
    key, elements = args[0], args[1:]
    store_entry = STORE.get(key)
    lst = store_entry.value if store_entry is not None else []

    lst.extend(elements)
    STORE[key] = StoreEntry(value=lst, expiry_time=None, redis_type='list')

    event_list = WAITERS.get(key, [])
    if event_list: 
        event_list[0].set()

    return f':{len(lst)}\r\n'.encode()


def run_lpush(args: list[str]) -> bytes:
    '''Prepend one or more elements to the head of a list.

    Args:
        args: The list of arguments where args[0] is the key and args[1:] are
              the elements to prepend.

    Returns:
        The length of the list after the push as a RESP integer.
    '''
    key, elements = args[0], args[1:]
    store_entry = STORE.get(key)
    lst = store_entry.value if store_entry is not None else []

    # Reverse elements, prepend them to the existing list, and store the result. 
    lst = elements[::-1] + lst  
    STORE[key] = StoreEntry(value=lst, expiry_time=None, redis_type='list')

    return f':{len(lst)}\r\n'.encode()


def run_lpop(args: list[str]) -> bytes:
    '''Remove and return one or more elements from the head of a list.

    Args:
        args: The list of arguments where args[0] is the key and the optional
              args[1] specifies the number of elements to pop.

    Returns:
        The popped element as a RESP bulk string, a RESP array of popped elements
        if a count was specified, or a null bulk string if the list is empty.
    '''
    key = args[0]
    store_entry = STORE.get(key)
    lst = store_entry.value if store_entry is not None else []
    
    if not lst:  
        return b'$-1\r\n'

    if len(args) == 1:  # No size argument provided; pop the first index only.
        elmt = lst.pop(0)
        STORE[key] = StoreEntry(value=lst, expiry_time=None, redis_type='list')
        return f'${len(elmt)}\r\n{elmt}\r\n'.encode()

    num = int(args[1])
    num_elmts_to_pop = min(num, len(lst))  # Refactored version (below)

    # if num > len(lst): 
    #     num_elmts_to_pop = len(lst) 
    # else: 
    #     num_elmts_to_pop = num
    
    # Refactored version (below)
    popped_elmts = lst[:num_elmts_to_pop]
    lst = lst[num_elmts_to_pop:]

    # popped_elmts = []
    # for elmt in lst[0:num_elmts_to_pop]: 
    #     popped_elmts.append(lst.pop(0))

    STORE[key] = StoreEntry(value=lst, expiry_time=None, redis_type='list')

    resp_array = [f'*{len(popped_elmts)}\r\n']
    for elmt in popped_elmts:
        resp_array.append(f'${len(elmt)}\r\n')
        resp_array.append(f'{elmt}\r\n')

    return ''.join(resp_array).encode()


async def run_blpop(args: list[str]) -> bytes:
    '''Remove and return the first element of a list, blocking if the list is empty.

    Args:
        args: The list of arguments where args[0] is the key and args[1] is the
              timeout in seconds. A timeout of 0 blocks indefinitely.

    Returns:
        A two-element RESP array containing the key and the popped element,
        or a null array if the timeout expires before an element is available.
    '''
    key = args[0]
    store_entry = STORE.get(key)
    lst = store_entry.value if store_entry is not None else []

    if not lst:
        event = asyncio.Event()
        event_list = WAITERS.get(key, [])
        event_list.append(event)
        WAITERS[key] = event_list
        
        timeout = float(args[1])
        timeout = None if timeout == 0 else timeout

        # Block until an element is pushed or the timeout expires.
        try:  
            await asyncio.wait_for(event.wait(), timeout)  
            lst = STORE[key].value  # Re-fetch the updated list after being unblocked. 
        except asyncio.TimeoutError:
            return b'*-1\r\n'
    else:
        event_list = WAITERS.get(key, [])
        if event_list:
            event_list.pop(0)  # Remove the handled event. 
            WAITERS[key] = event_list  # Update the events list and store it. 
            
    elmt = lst.pop(0)
    STORE[key] = StoreEntry(value=lst, expiry_time=None, redis_type='list')

    return f'*2\r\n${len(key)}\r\n{key}\r\n${len(elmt)}\r\n{elmt}\r\n'.encode()


def run_lrange(args: list[str]) -> bytes:
    '''Return a range of elements from a list.

    Args:
        args: The list of arguments where args[0] is the key, args[1] is the
              start index, and args[2] is the stop index. Negative indexes are
              supported.

    Returns:
        A RESP array of elements within the specified range.
    '''
    key = args[0]
    store_entry = STORE.get(key)

    # Return an empty array if the key doesn't exist.
    if store_entry is None:
        return b'*0\r\n'  
    
    lst = store_entry.value
    lst_size = len(lst)

    # Convert negative indexes to their positive equivalents using the list length as the offset.
    # Clamp any out-of-bounds negative index to 0.
    start, stop = int(args[1]), int(args[2])
    start = max(0, lst_size + start if start < 0 else start)
    stop = max(0, lst_size + stop if stop < 0 else stop)

    # Clamp stop to the last valid index if it exceeds the list length.
    if stop >= lst_size: 
        stop = lst_size - 1

    # Return an empty array if start is out of bounds or greater than stop.
    if start >= lst_size or start > stop: 
        return b'*0\r\n'  

    sliced = lst[start:stop+1]
    resp_array = [f'*{len(sliced)}\r\n']

    for elmt in sliced:
        resp_array.append(f'${len(elmt)}\r\n')
        resp_array.append(f'{elmt}\r\n')
    
    # Join the RESP parts into a single continuous bytes object before sending over the socket.
    return ''.join(resp_array).encode()


def run_llen(args: list[str]) -> bytes:
    '''Return the length of a list.

    Args:
        args: The list of arguments where args[0] is the key.

    Returns:
        The length of the list as a RESP integer, or 0 if the key doesn't exist.
    '''
    key = args[0]
    store_entry = STORE.get(key)
    lst = store_entry.value if store_entry is not None else []

    if not lst:
        return b':0\r\n'
    
    return f':{len(lst)}\r\n'.encode()


def parse_stream_id(stream_id: str) -> tuple[int, int | str]:
    '''Parse a stream ID string into its millisecond time and sequence number components.

    Args:
        stream_id: The stream ID string in the format <ms_time>-<seq_num>,
                   or * to auto-generate using the current time.

    Returns:
        A tuple of (ms_time, seq_num) where seq_num may be an int or '*'.
    '''
    if stream_id == '*':
        return int(time() * 1000), '*'
    
    # No sequence number provided
    if '-' not in stream_id:
        stream_id = stream_id + '-0'

    parts = stream_id.split('-')
    ms_time = int(parts[0])
    seq_num = parts[1] if parts[1] == '*' else int(parts[1])

    return ms_time, seq_num


def run_xadd(args: list[str]) -> bytes:
    '''Append a new entry to a stream.

    Args:
        args: The list of arguments where args[0] is the key, args[1] is the
              entry ID, and args[2:] are alternating field-value pairs.

    Returns:
        The ID of the newly added entry as a RESP bulk string, or an error
        if the ID is invalid.
    '''
    stream_id = args[1]
    kv_pairs = args[2:]
    keys = kv_pairs[0::2]
    values = kv_pairs[1::2]
    fields = dict(zip(keys, values))  # Refactored version (below)

    # fields = {}
    # for i in range(len(keys)): 
    #     fields[keys[i]] = values[i]

    ms_time, seq_num = parse_stream_id(stream_id)

    if ms_time == 0 and seq_num == 0:  
        return b'-ERR The ID specified in XADD must be greater than 0-0\r\n'
    
    key = args[0]
    store_entry = STORE.get(key)

    if store_entry is None:
        if seq_num == '*': 
            seq_num = 1 if ms_time == 0 else 0

        stream_id = f'{ms_time}-{seq_num}'
        stream = [(stream_id, fields)]
        STORE[key] = StoreEntry(value=stream, expiry_time=None, redis_type='stream')
    else:
        last_stream_entry = store_entry.value[-1]  
        last_entry_id_parts = last_stream_entry[0].split('-')
        last_entry_ms_time = int(last_entry_id_parts[0])
        last_entry_seq_num = int(last_entry_id_parts[1])

        if seq_num == '*': 
            if ms_time == 0: 
                seq_num = 1
            elif ms_time == last_entry_ms_time: 
                seq_num = last_entry_seq_num + 1
            elif ms_time != 0 and ms_time != last_entry_ms_time:
                seq_num = 0
        
        assert isinstance(seq_num, int), "seq_num should be an int, not '*'"

        if ms_time < last_entry_ms_time or (ms_time == last_entry_ms_time and seq_num <= last_entry_seq_num):
            return b'-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n'
        
        stream = store_entry.value
        stream_id = f'{ms_time}-{seq_num}'
        stream.append((stream_id, fields))
        STORE[key] = StoreEntry(value=stream, expiry_time=None, redis_type='stream')

    event_list = WAITERS.get(key, [])  # Notify `XRANGE` that an entry has been added.
    if event_list:
        event_list[0].set()  # Index 0 contains the longest-waiting client request. 

    return f'${len(stream_id)}\r\n{stream_id}\r\n'.encode()


def build_entries_array(args: list[tuple[str, list[str]]]) -> list[str]:
    '''Build a RESP array of stream entries.

    Args:
        args: A list of tuples where each tuple contains a stream ID and a
              flat list of alternating field keys and values.

    Returns:
        A list of RESP-encoded strings representing the entries array.
    '''
    entries = [f'*{len(args)}\r\n']

    for stream_id, kv_list in args:
        entries.append('*2\r\n')
        entries.append(f'${len(stream_id)}\r\n{stream_id}\r\n')
        entries.append(f'*{len(kv_list)}\r\n')

        for elmt in kv_list: 
            resp_elmt = f'${len(elmt)}\r\n{elmt}\r\n'
            entries.append(resp_elmt)

    return entries


def run_xrange(args: list[str]) -> bytes:
    '''Return a range of entries from a stream between two IDs, inclusive.

    Args:
        args: The list of arguments where args[0] is the key, args[1] is the
              start ID, and args[2] is the end ID. Use - for the minimum ID
              and + for the maximum ID.

    Returns:
        A RESP array of matching stream entries.
    '''
    key = args[0]
    store_entry = STORE.get(key)

    if store_entry is None:
        return b'*0\r\n'

    # '-' as the start ID indicates the beginning of the stream; default to the minimum possible ID.
    if args[1] == '-': 
        start_id_ms_time, start_id_seq_num = 0, 0
    else: 
        start_id_ms_time, start_id_seq_num = parse_stream_id(args[1])  # If start ID has no sequence number, default to 0.

    # '+' as the end ID indicates the end of the stream; default to the maximum possible int value.
    if args[2] == '+':
        end_id_ms_time, end_id_seq_num = sys.maxsize, sys.maxsize
    else: 
        end_id_ms_time, end_id_seq_num = parse_stream_id(args[2])  # If end ID has no sequence number, default to the maximum possible int value.
        if '-' not in args[2]:
            end_id_seq_num = sys.maxsize
    
    stream = store_entry.value  
    matches = []
    for entry in stream: 
        entry_id_ms_time, entry_id_seq_num = parse_stream_id(entry[0])
        entry_fields = entry[1] 

        if (start_id_ms_time, start_id_seq_num) <= (entry_id_ms_time, entry_id_seq_num) <= (end_id_ms_time, end_id_seq_num):
            kv_list = []
            for field_key, value in entry_fields.items():
                kv_list.append(field_key)
                kv_list.append(value)
            matches.append((entry[0], kv_list))  
        
    resp_array = build_entries_array(matches)

    return ''.join(resp_array).encode()


def get_entry_matches(stream: list[tuple[str, dict[str, str]]], stream_id: str) -> list[tuple[str, list[str]]]:
    '''Return all stream entries with an ID greater than the given stream ID.

    Args:
        stream: The list of stream entries to search.
        stream_id: The exclusive lower bound ID.

    Returns:
        A list of tuples where each tuple contains an entry ID and a flat
        list of alternating field keys and values.
    '''
    # Exclusive comparison; entries with an ID equal to stream_id are not included.
    start_ms_time, start_seq_num = parse_stream_id(stream_id)

    matches = []
    for entry in stream: 
        entry_id_ms_time, entry_id_seq_num = parse_stream_id(entry[0])
        entry_fields = entry[1] 

        if (entry_id_ms_time, entry_id_seq_num) > (start_ms_time, start_seq_num):
            kv_list = []
            for field_key, value in entry_fields.items():
                kv_list.append(field_key)
                kv_list.append(value)
            matches.append((entry[0], kv_list))

    return matches


def build_stream_response(resp_array: list[str], key: str, matches: list[tuple[str, list[str]]]) -> None:
    '''Append a stream's key and entries to an existing RESP array in place.

    Args:
        resp_array: The RESP array to append to.
        key: The stream key.
        matches: The list of matching stream entries.
    '''
    entries = build_entries_array(matches)
    resp_array.append('*2\r\n')  # Each stream is a 2-element array
    resp_array.append(f'${len(key)}\r\n{key}\r\n')  # Stream key
    resp_array.extend(entries)  # RESP array (already includes its own array header)


async def run_xread(args: list[str]) -> bytes:
    '''Read entries from one or more streams starting after a given ID.

    Args:
        args: The list of arguments including optional BLOCK and timeout,
              followed by STREAMS, stream keys, and their respective start IDs.

    Returns:
        A RESP array of streams and their matching entries, or a null array
        if the timeout expires with no new entries.
    '''
    timeout = None
    if 'block' in args:
        stream_args = args[3:]  # Skip 'BLOCK', timeout, and 'STREAMS'
        mid = len(stream_args) // 2
        keys = stream_args[:mid]
        stream_ids = stream_args[mid:]
        timeout = float(args[1]) // 1000  # Convert milliseconds to seconds.
    else: 
        stream_args = args[1:]  # Skip 'STREAMS'
        mid = len(stream_args) // 2
        keys = stream_args[:mid]
        stream_ids = stream_args[mid:]

    resp_array = [f'*{len(keys)}\r\n']  # 'keys' indicates the number of streams.

    for i, key in enumerate(keys): 
        store_entry = STORE.get(key)

        if store_entry is None:
            return b'*0\r\n'

        stream = store_entry.value

        if stream_ids[i] == '$':
            stream_id = '0-0' if not stream else stream[-1][0]
        else: 
            stream_id = stream_ids[i]

        matches = get_entry_matches(stream, stream_id)  

        if 'block' in args:
            if not matches: 
                event = asyncio.Event()
                event_list = WAITERS.get(key, [])
                event_list.append(event)
                WAITERS[key] = event_list
                
                timeout = None if timeout == 0 else timeout
                try:
                    await asyncio.wait_for(event.wait(), timeout)

                    store_entry = STORE.get(key)  # Re-fetch the stream after one or more entries has been added.

                    if store_entry is None:
                        return b'*0\r\n'
                    
                    matches = get_entry_matches(store_entry.value, stream_id)
                    build_stream_response(resp_array, key, matches)
                    
                    event_list = WAITERS.get(key, [])
                    if event_list:
                        event_list.pop(0)  # Remove the handled event. 
                        WAITERS[key] = event_list  # Update the events list and store it.

                    return ''.join(resp_array).encode()
                except asyncio.TimeoutError:
                    return b'*-1\r\n'
            else:
                build_stream_response(resp_array, key, matches)
                return ''.join(resp_array).encode()

        build_stream_response(resp_array, key, matches)

    return ''.join(resp_array).encode()


def run_incr(args: list[str]) -> bytes:
    '''Increment the integer value stored at a key by one.

    Args:
        args: The list of arguments where args[0] is the key to increment.

    Returns:
        The new value as a RESP integer, or an error if the value is not
        a valid integer.
    '''
    key = args[0]
    store_entry = STORE.get(key)

    if store_entry is None:
        result = 1
    else:
        try:
            result = int(store_entry.value) + 1
        except ValueError:
            return b'-ERR value is not an integer or out of range\r\n'

    STORE[key] = StoreEntry(value=str(result), expiry_time=None, redis_type='string')

    return f':{result}\r\n'.encode()


def run_info(args: list[str]) -> bytes:
    '''Return server information and statistics for the replication section.

    Returns:
        The replication details as a RESP bulk string.
    '''
    content = '# Replication\r\n'
    for key, value in SERVER_INFO.items():
        content += f'{key}:{value}\r\n'
    return f'${len(content)}\r\n{content}\r\n'.encode()


def run_replconf(args: list[str]) -> bytes:
    '''Acknowledge a REPLCONF command from a replica.

    Returns:
        +OK on success.
    '''
    return b'+OK\r\n'


def run_psync(args: list[str]) -> bytes:
    '''Initiate full resynchronization with a replica.

    Returns:
        A +FULLRESYNC response containing the replication ID and offset.
    '''
    return f'+FULLRESYNC {SERVER_INFO['master_replid']} {SERVER_INFO['master_repl_offset']}\r\n'.encode()


COMMANDS = {
    'PING': run_ping,
    'ECHO': run_echo,
    'TYPE': run_type,
    'SET': run_set,
    'GET': run_get,
    'RPUSH': run_rpush,
    'LPUSH': run_lpush,
    'LPOP': run_lpop,
    'BLPOP': run_blpop,
    'LRANGE': run_lrange,
    'LLEN': run_llen,
    'XADD': run_xadd,
    'XRANGE': run_xrange,
    'XREAD': run_xread,
    'INCR': run_incr,
    'INFO': run_info,
    'REPLCONF': run_replconf,
    'PSYNC': run_psync,
}
