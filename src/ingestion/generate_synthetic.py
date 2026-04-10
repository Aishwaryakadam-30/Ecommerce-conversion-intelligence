"""
Synthetic 2025 Data Generator
==============================
Author: Aishwarya Kadam
Project: E-commerce Conversion Intelligence Platform

Generates realistic synthetic e-commerce clickstream events for 2025
that match the structure of the real 2024 dataset. This solves the
low session count issue and adds recent dates to our data.
"""

import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random

fake = Faker()
Faker.seed(42)
random.seed(42)
np.random.seed(42)


# Configuration
EVENT_TYPES = ['page_view', 'product_view', 'click', 'add_to_cart', 'purchase', 'login', 'logout']

# Realistic event transition probabilities
# These simulate a real user journey
EVENT_PROBABILITIES = {
    'page_view': 0.25,
    'product_view': 0.22,
    'click': 0.18,
    'add_to_cart': 0.12,
    'purchase': 0.08,
    'login': 0.08,
    'logout': 0.07
}

# Device and traffic source distributions
DEVICE_TYPES = ['mobile', 'desktop', 'tablet']
DEVICE_WEIGHTS = [0.55, 0.35, 0.10]

TRAFFIC_SOURCES = ['organic', 'paid_search', 'social', 'direct', 'email', 'referral']
TRAFFIC_WEIGHTS = [0.30, 0.25, 0.20, 0.15, 0.05, 0.05]


def generate_user_session(user_id: int, session_id: int, start_time: datetime) -> list:
    """
    Generate a realistic sequence of events for one user session.
    
    Each session follows a typical user journey pattern:
    login -> page_view -> product_view -> click -> (add_to_cart) -> (purchase) -> logout
    
    Returns:
        List of event dictionaries
    """
    events = []
    current_time = start_time
    
    # Session length: weighted random between 3 and 25 events
    session_length = int(np.random.gamma(shape=2, scale=3) + 2)
    session_length = min(max(session_length, 3), 25)
    
    # Decide user intent for this session (affects conversion)
    intent = random.choices(['browser', 'researcher', 'buyer'], weights=[0.60, 0.25, 0.15])[0]
    
    # Track state
    cart_added = False
    has_purchased = False
    
    for i in range(session_length):
        # Start session with login
        if i == 0:
            event_type = 'login'
        # End session with logout
        elif i == session_length - 1:
            event_type = 'logout'
        # Middle events depend on intent
        else:
            if intent == 'buyer' and not has_purchased and i > session_length * 0.6:
                # Buyers likely to convert
                event_type = random.choices(
                    ['product_view', 'add_to_cart', 'purchase', 'click'],
                    weights=[0.2, 0.3, 0.4, 0.1]
                )[0]
            elif intent == 'researcher':
                # Researchers view a lot but convert less
                event_type = random.choices(
                    ['page_view', 'product_view', 'click', 'add_to_cart'],
                    weights=[0.3, 0.4, 0.2, 0.1]
                )[0]
            else:
                # Browsers mostly view
                event_type = random.choices(
                    ['page_view', 'product_view', 'click'],
                    weights=[0.5, 0.35, 0.15]
                )[0]
            
            # Track state
            if event_type == 'add_to_cart':
                cart_added = True
            if event_type == 'purchase':
                has_purchased = True
        
        # Time between events: 5 seconds to 5 minutes
        time_delta = timedelta(seconds=random.randint(5, 300))
        current_time += time_delta
        
        # Generate the event
        event = {
            'user_id': user_id,
            'session_id': session_id,
            'timestamp': current_time,
            'event_type': event_type,
            'product_id': f'prod_{random.randint(1000, 9999)}' if event_type in ['product_view', 'click', 'add_to_cart', 'purchase'] else None,
            'amount': round(random.uniform(10, 500), 2) if event_type == 'purchase' else None,
            'outcome': 'purchase' if event_type == 'purchase' else None,
            'device_type': random.choices(DEVICE_TYPES, weights=DEVICE_WEIGHTS)[0],
            'traffic_source': random.choices(TRAFFIC_SOURCES, weights=TRAFFIC_WEIGHTS)[0]
        }
        events.append(event)
    
    return events


def generate_synthetic_data(
    num_users: int = 5000,
    sessions_per_user_avg: float = 3.5,
    start_date: str = '2025-01-01',
    end_date: str = '2025-12-31'
) -> pd.DataFrame:
    """
    Generate a full synthetic dataset.
    
    Args:
        num_users: Number of unique users to generate
        sessions_per_user_avg: Average number of sessions per user
        start_date: Start of date range (YYYY-MM-DD)
        end_date: End of date range (YYYY-MM-DD)
    
    Returns:
        DataFrame of synthetic clickstream events
    """
    print("=" * 50)
    print("GENERATING SYNTHETIC 2025 DATA")
    print("=" * 50)
    print(f"Target users: {num_users:,}")
    print(f"Avg sessions per user: {sessions_per_user_avg}")
    print(f"Date range: {start_date} to {end_date}")
    print()
    
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    date_range_seconds = int((end_dt - start_dt).total_seconds())
    
    all_events = []
    session_counter = 100000  # Start session IDs high to avoid collisions with real data
    
    for user_id in range(10000, 10000 + num_users):
        # Each user has 1 to N sessions
        num_sessions = max(1, int(np.random.poisson(sessions_per_user_avg)))
        
        for _ in range(num_sessions):
            # Random session start time within the date range
            random_seconds = random.randint(0, date_range_seconds)
            session_start = start_dt + timedelta(seconds=random_seconds)
            
            # Generate session events
            session_events = generate_user_session(user_id, session_counter, session_start)
            all_events.extend(session_events)
            session_counter += 1
        
        # Progress indicator
        if (user_id - 10000 + 1) % 1000 == 0:
            print(f"  Generated {user_id - 10000 + 1:,} users so far...")
    
    df = pd.DataFrame(all_events)
    
    print()
    print(f"Generated {len(df):,} total events")
    print(f"Unique users: {df['user_id'].nunique():,}")
    print(f"Unique sessions: {df['session_id'].nunique():,}")
    print("=" * 50)
    
    return df


def save_synthetic_data(df: pd.DataFrame, output_path: str):
    """Save synthetic data to CSV."""
    df.to_csv(output_path, index=False)
    print(f"Saved to: {output_path}")


if __name__ == "__main__":
    # Generate and save when script is run directly
    synthetic_df = generate_synthetic_data(
        num_users=5000,
        sessions_per_user_avg=3.5
    )
    save_synthetic_data(synthetic_df, 'data/synthetic/synthetic_2025.csv')
