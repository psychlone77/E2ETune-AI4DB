#!/usr/bin/env python3
"""
Script to update the <time> value in all SmallBank XML config files.
Changes <time>60</time> to <time>30</time> in the works section.
"""

import os
import glob
import xml.etree.ElementTree as ET

def update_time_in_xml(file_path, new_time=30):
    """Update the time value in a SmallBank XML config file using ElementTree."""
    try:
        # Parse the XML file
        tree = ET.parse(file_path)
        root = tree.getroot()
        
        # Find all <time> elements under <works>
        updated = False
        for time_elem in root.findall('.//works/work/time'):
            old_value = time_elem.text
            if old_value and int(old_value.strip()) != new_time:
                time_elem.text = str(new_time)
                updated = True
        
        # Only write if something changed
        if updated:
            # Write back to file, preserving the XML declaration
            tree.write(file_path, encoding='utf-8', xml_declaration=True)
            print(f"✓ Updated: {file_path}")
            return True
        else:
            print(f"- No change needed: {file_path}")
            return False
            
    except Exception as e:
        print(f"✗ Error processing {file_path}: {e}")
        return False

def main():
    # Directory containing SmallBank config files
    config_dir = '/home/E2ETune-AI4DB/oltp_workloads/smallbank'
    
    # Find all XML files in the directory
    xml_files = glob.glob(os.path.join(config_dir, '*.xml'))
    
    if not xml_files:
        print(f"No XML files found in {config_dir}")
        return
    
    print(f"Found {len(xml_files)} XML file(s) in {config_dir}")
    print(f"Updating <time> to 30 seconds...\n")
    
    updated_count = 0
    for xml_file in sorted(xml_files):
        if update_time_in_xml(xml_file, new_time=30):
            updated_count += 1
    
    print(f"\n{'='*50}")
    print(f"Summary: Updated {updated_count} out of {len(xml_files)} file(s)")
    print(f"{'='*50}")

if __name__ == '__main__':
    main()
