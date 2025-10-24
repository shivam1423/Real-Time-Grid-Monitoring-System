"""
Energy Grid Data Generator
Generates realistic energy grid data with time-based patterns, anomalies, and regional variations.
"""

import json
import random
import time
from datetime import datetime, timezone
from typing import Dict, List
import yaml
from confluent_kafka import Producer
import math
import sys
import os

# Add config directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'config'))
from environment_config import get_kafka_bootstrap_servers


class EnergyGridGenerator:
    """Simulates realistic energy grid data for streaming pipeline"""
    
    def __init__(self, config_path: str = 'config/energy_grid_config.yaml'):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.regions = self.config['regions']
        self.generation_types = self.config['generation_types']
        self.carbon_coefficients = self.config['carbon_coefficients']
        self.generation_costs = self.config['generation_costs']
        self.base_capacity = self.config['regional_base_capacity']
        self.thresholds = self.config['thresholds']
        
        # Kafka producer setup with error handling
        kafka_bootstrap = get_kafka_bootstrap_servers()
        self.producer = Producer({
            'bootstrap.servers': kafka_bootstrap,
            'compression.type': 'gzip',
            'retries': 3,
            'retry.backoff.ms': 100,
            'request.timeout.ms': 30000,
            'delivery.timeout.ms': 120000
        })
        
        self.topic = self.config['streaming']['kafka_topic']
        
    def delivery_callback(self, err, msg):
        """Callback for message delivery confirmation"""
        if err is not None:
            print(f"Message delivery failed: {err}")
        # else:
        #     print(f"Message delivered to {msg.topic()} [{msg.partition()}]")
        
    def get_hour_multiplier(self, hour: int) -> float:
        """Calculate demand multiplier based on time of day"""
        # Peak hours: 8-10 AM and 6-9 PM
        if 8 <= hour <= 10 or 18 <= hour <= 21:
            return random.uniform(1.3, 1.5)
        # Moderate hours: 11 AM - 5 PM
        elif 11 <= hour <= 17:
            return random.uniform(1.1, 1.3)
        # Low demand hours: 11 PM - 6 AM
        elif hour >= 23 or hour <= 6:
            return random.uniform(0.6, 0.8)
        # Normal hours
        else:
            return random.uniform(0.9, 1.1)
    
    def get_solar_output(self, hour: int, base_capacity: float) -> float:
        """Calculate solar generation based on time of day"""
        # No solar at night
        if hour < 6 or hour > 20:
            return 0.0
        
        # Peak solar hours: 11 AM - 3 PM
        if 11 <= hour <= 15:
            capacity_factor = random.uniform(0.7, 0.9)
        # Morning/evening ramp
        elif 6 <= hour <= 10 or 16 <= hour <= 20:
            distance_from_peak = min(abs(hour - 13), 7)
            capacity_factor = random.uniform(0.2, 0.6) * (1 - distance_from_peak / 10)
        else:
            capacity_factor = 0.0
        
        return base_capacity * 0.15 * capacity_factor  # 15% of total capacity is solar
    
    def get_wind_output(self, hour: int, base_capacity: float) -> float:
        """Calculate wind generation with some variability"""
        # Wind is more consistent but still variable
        base_wind = base_capacity * 0.20  # 20% of capacity is wind
        # Wind tends to be stronger at night
        if 20 <= hour or hour <= 6:
            capacity_factor = random.uniform(0.6, 0.85)
        else:
            capacity_factor = random.uniform(0.4, 0.7)
        
        return base_wind * capacity_factor
    
    def generate_generation_mix(self, region: str, hour: int, demand: float) -> Dict[str, float]:
        """Generate realistic generation mix for a region"""
        base_capacity = self.base_capacity[region]
        
        # Calculate renewable generation first
        solar_mw = self.get_solar_output(hour, base_capacity)
        wind_mw = self.get_wind_output(hour, base_capacity)
        
        # Nuclear is baseload - runs at constant output
        nuclear_mw = base_capacity * 0.15 * random.uniform(0.9, 0.95)
        
        # Hydro is flexible but limited
        hydro_mw = base_capacity * 0.10 * random.uniform(0.5, 0.8)
        
        # Calculate remaining demand to be met
        renewable_and_base = solar_mw + wind_mw + nuclear_mw + hydro_mw
        remaining_demand = max(0, demand - renewable_and_base)
        
        # Split remaining between natural gas and coal
        # Natural gas is more flexible and preferred for ramping
        gas_ratio = random.uniform(0.6, 0.75)
        natural_gas_mw = remaining_demand * gas_ratio
        coal_mw = remaining_demand * (1 - gas_ratio)
        
        generation_mw = {
            'Solar': round(solar_mw, 2),
            'Wind': round(wind_mw, 2),
            'Natural_Gas': round(natural_gas_mw, 2),
            'Coal': round(coal_mw, 2),
            'Nuclear': round(nuclear_mw, 2),
            'Hydro': round(hydro_mw, 2)
        }
        
        total_generation = sum(generation_mw.values())
        
        # Calculate percentages
        generation_mix_percent = {
            source: round((mw / total_generation * 100), 2) if total_generation > 0 else 0
            for source, mw in generation_mw.items()
        }
        
        return generation_mw, generation_mix_percent, total_generation
    
    def calculate_carbon_intensity(self, generation_mix_percent: Dict[str, float]) -> float:
        """Calculate weighted carbon intensity"""
        total_carbon = sum(
            generation_mix_percent[source] * self.carbon_coefficients[source] / 100
            for source in self.generation_types
        )
        return round(total_carbon, 2)
    
    def calculate_generation_cost(self, generation_mw: Dict[str, float]) -> float:
        """Calculate total generation cost per hour"""
        total_cost = sum(
            generation_mw[source] * self.generation_costs[source]
            for source in self.generation_types
        )
        return round(total_cost, 2)
    
    def detect_anomaly(self, data: Dict) -> tuple:
        """Detect grid anomalies and set alert level"""
        is_anomaly = False
        alert_level = 'NORMAL'
        
        # Check frequency deviation
        freq_deviation = abs(data['grid_frequency_hz'] - 60.0)
        if freq_deviation > self.thresholds['frequency_deviation_hz']:
            is_anomaly = True
            alert_level = 'CRITICAL' if freq_deviation > 0.025 else 'WARNING'
        
        # Check reserve margin
        if data['reserve_margin_percent'] < self.thresholds['min_reserve_margin_percent']:
            is_anomaly = True
            alert_level = 'CRITICAL'
        
        # Check voltage
        voltage = data['voltage_kv']
        if voltage < self.thresholds['voltage_min_kv'] or voltage > self.thresholds['voltage_max_kv']:
            is_anomaly = True
            alert_level = 'WARNING' if alert_level == 'NORMAL' else 'CRITICAL'
        
        return is_anomaly, alert_level
    
    def inject_anomaly(self, data: Dict, anomaly_prob: float = 0.05) -> Dict:
        """Randomly inject anomalies for testing with bounds checking"""
        if random.random() < anomaly_prob:
            anomaly_type = random.choice(['frequency', 'voltage', 'overload'])
            
            if anomaly_type == 'frequency':
                # Frequency deviation with realistic bounds
                deviation = random.choice([1, -1]) * random.uniform(0.02, 0.05)
                new_frequency = data['grid_frequency_hz'] + deviation
                # Keep within realistic grid frequency bounds (50-70 Hz)
                data['grid_frequency_hz'] = max(50.0, min(70.0, new_frequency))
                
            elif anomaly_type == 'voltage':
                # Voltage instability with realistic bounds
                voltage_range = random.choice([
                    (320, 334),  # Low voltage
                    (356, 370)   # High voltage
                ])
                data['voltage_kv'] = random.uniform(voltage_range[0], voltage_range[1])
                
            elif anomaly_type == 'overload':
                # Demand spike with reasonable limits
                spike_multiplier = random.uniform(1.5, 1.8)
                new_demand = data['demand_mw'] * spike_multiplier
                # Cap at 200% of base capacity to prevent unrealistic values
                max_demand = self.base_capacity[data['region']] * 2.0
                data['demand_mw'] = min(new_demand, max_demand)
        
        return data
    
    def generate_grid_data(self, region: str) -> Dict:
        """Generate a single grid data point for a region"""
        # Use current time for realistic data generation
        now = datetime.now(timezone.utc)
        hour = now.hour
        
        # Base demand with time-of-day patterns
        base_demand = self.base_capacity[region] * 0.7  # 70% of capacity on average
        time_multiplier = self.get_hour_multiplier(hour)
        
        # Add temperature effect (simplified)
        temperature = random.uniform(15, 35)  # Celsius
        temp_effect = 1.0
        if temperature > 30:
            # High temps increase AC demand
            temp_effect = 1 + (temperature - 30) * 0.02
        elif temperature < 5:
            # Low temps increase heating demand
            temp_effect = 1 + (5 - temperature) * 0.015
        
        demand_mw = base_demand * time_multiplier * temp_effect
        demand_mw += random.uniform(-500, 500)  # Random fluctuation
        
        # Generate power mix
        generation_mw, generation_mix_percent, total_generation = \
            self.generate_generation_mix(region, hour, demand_mw)
        
        # Grid stability metrics
        grid_frequency = 60.0 + random.uniform(-0.005, 0.005)
        voltage_kv = random.uniform(340, 350)  # Normal operating range
        
        # Calculate metrics
        reserve_margin = ((total_generation - demand_mw) / demand_mw * 100) if demand_mw > 0 else 0
        carbon_intensity = self.calculate_carbon_intensity(generation_mix_percent)
        generation_cost = self.calculate_generation_cost(generation_mw)
        
        # Calculate renewable percentage
        renewable_mw = generation_mw['Solar'] + generation_mw['Wind'] + generation_mw['Hydro']
        renewable_percentage = (renewable_mw / total_generation * 100) if total_generation > 0 else 0
        
        data = {
            'timestamp': now.isoformat(),
            'region': region,
            'grid_id': f"{region.upper()}-GRID-{random.randint(1, 5)}",
            'demand_mw': round(demand_mw, 2),
            'total_generation_mw': round(total_generation, 2),
            'generation_mix_percent': generation_mix_percent,
            'generation_mw': generation_mw,
            'grid_frequency_hz': round(grid_frequency, 4),
            'voltage_kv': round(voltage_kv, 2),
            'carbon_intensity_gco2_kwh': carbon_intensity,
            'reserve_margin_percent': round(reserve_margin, 2),
            'temperature_celsius': round(temperature, 1),
            'hour_of_day': hour,
            'renewable_percentage': round(renewable_percentage, 2),
            'generation_cost_per_hour': generation_cost,
            'is_anomaly': False,
            'alert_level': 'NORMAL'
        }
        
        # Inject anomalies occasionally
        data = self.inject_anomaly(data)
        
        # Detect anomalies
        is_anomaly, alert_level = self.detect_anomaly(data)
        data['is_anomaly'] = is_anomaly
        data['alert_level'] = alert_level
        
        return data
    
    def start_streaming(self, interval_seconds: int = 5):
        """Start continuous data generation and streaming to Kafka"""
        print(f"Starting Energy Grid data streaming to Kafka topic: {self.topic}")
        print(f"Streaming interval: {interval_seconds} seconds")
        print(f"Monitoring regions: {', '.join(self.regions)}")
        print("-" * 80)
        
        try:
            while True:
                # Generate data for all regions
                for region in self.regions:
                    data = self.generate_grid_data(region)
                    
                    # Send to Kafka with error handling
                    try:
                        self.producer.produce(
                            topic=self.topic,
                            value=json.dumps(data).encode('utf-8'),
                            key=region.encode('utf-8'),
                            callback=self.delivery_callback
                        )
                        self.producer.poll(0)  # Trigger delivery
                        
                        # Log if anomaly detected
                        if data['is_anomaly']:
                            print(f"ANOMALY DETECTED in {region}: {data['alert_level']} - "
                                  f"Freq: {data['grid_frequency_hz']} Hz, "
                                  f"Reserve: {data['reserve_margin_percent']}%")
                        else:
                            print(f"{region}: Demand={data['demand_mw']:.0f}MW, "
                                  f"Renewable={data['renewable_percentage']:.1f}%, "
                                  f"Carbon={data['carbon_intensity_gco2_kwh']:.0f}g/kWh")
                    except Exception as e:
                        print(f"Failed to send data for {region}: {e}")
                        # Continue with other regions even if one fails
                        continue
                
                self.producer.flush(timeout=5)
                print("-" * 80)
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            print("\nStopping data generator...")
        except Exception as e:
            print(f"Error in data generator: {e}")
            raise
        finally:
            # Ensure producer is always closed
            self.producer.flush(timeout=10)
            # confluent_kafka Producer doesn't have a close method


if __name__ == "__main__":
    import sys
    
    config_path = sys.argv[1] if len(sys.argv) > 1 else '../config/energy_grid_config.yaml'
    interval = int(sys.argv[2]) if len(sys.argv) > 2 else 5
    
    generator = EnergyGridGenerator(config_path)
    generator.start_streaming(interval)


