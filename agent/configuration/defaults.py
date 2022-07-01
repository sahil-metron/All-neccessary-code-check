GLOBAL_DEFAULTS = {
    'queue_max_size_in_mb': 1024,
    'queue_max_size_in_messages': 10000,
    'queue_max_elapsed_time_in_sec': 60,
    'queue_wrap_max_size_in_messages': 100,
    'queue_generate_metrics': False,
    'balanced_output': False,
    'debug': False,
    'multiprocessing': True,
    'max_consumers_for_multiprocessing': 2,
    'max_consumers_for_multithreading': 500,
    'sender_concurrent_connections': 1,
    'sender_stats_period_in_sec': 300,
    'sender_activate_final_queue': False,
    'generate_collector_details': False,
    'devo_sender_threshold_for_using_gzip_in_transport_layer': 1.1,
    'devo_sender_compression_level': 6,
    'devo_sender_compression_buffer_in_bytes': 51200,
    'persistence': {
        'type': 'filesystem',
        'config': {
            'directory_name': 'state'
        }
    }
}
