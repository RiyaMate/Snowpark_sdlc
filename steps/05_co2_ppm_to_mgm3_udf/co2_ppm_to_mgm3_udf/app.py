#------------------------------------------------------------------------------
# Hands-On Lab: Data Engineering with Snowpark
# Script:       05_co2_ppm_to_mgm3_udf/app.py
# Author:       Riya
# Last Updated: 2025-02-27
#------------------------------------------------------------------------------

# SNOWFLAKE ADVANTAGE: Snowpark Python programmability
# SNOWFLAKE ADVANTAGE: Python UDFs (with third-party packages)
# SNOWFLAKE ADVANTAGE: SnowCLI for deployment

import sys

def main(co2_ppm: float, temp_c: float = 25.0, pressure_hpa: float = 1013.25) -> float:
    """
    Converts CO₂ concentration from ppm to mg/m³.
    
    Parameters:
        co2_ppm (float): CO₂ concentration in parts per million (ppm).
        temp_c (float): Temperature in degrees Celsius (default: 25°C).
        pressure_hpa (float): Atmospheric pressure in hPa (default: 1013.25 hPa).
    
    Returns:
        float: CO₂ concentration in mg/m³.
    """
    try:
        if co2_ppm is None:
            raise ValueError("CO2 PPM value is required.")

        return co2_ppm * (44.01 / 24.45) * (273.15 / (temp_c + 273.15)) * (pressure_hpa / 1013.25)
    
    except Exception as e:
        print(f"❌ Error: {e}")
        return None


# For local debugging
if __name__ == '__main__':
    if len(sys.argv) > 1:
        try:
            inputs = list(map(float, sys.argv[1:]))  # Convert CLI inputs to float
            print(main(*inputs))
        except ValueError:
            print("❌ Invalid input. Usage: python app.py <CO2_ppm> [temperature_C] [pressure_hPa]")
    else:
        print("Usage: python app.py <CO2_ppm> [temperature_C] [pressure_hPa]")
