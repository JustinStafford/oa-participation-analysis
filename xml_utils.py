import xml.etree.ElementTree as ET

def print_xml_structure(xml_element, indent=0):
    """
    Print XML structure in a readable format.
    
    Args:
        xml_element: XML element to print
        indent: Current indentation level
    """
    if xml_element is None:
        print("None")
        return
        
    # Print element tag and attributes
    print('  ' * indent + f'<{xml_element.tag}', end='')
    for key, value in xml_element.attrib.items():
        print(f' {key}="{value}"', end='')
    print('>')
    
    # Print text content if it exists and is not just whitespace
    if xml_element.text and xml_element.text.strip():
        print('  ' * (indent + 1) + xml_element.text.strip())
    
    # Recursively print child elements
    for child in xml_element:
        print_xml_structure(child, indent + 1)
    
    # Print closing tag
    print('  ' * indent + f'</{xml_element.tag}>')

def print_xml_pretty(xml_element):
    """
    Print XML in a pretty format with proper indentation.
    
    Args:
        xml_element: XML element to print
    """
    if xml_element is None:
        print("None")
        return
        
    # Convert ElementTree to string with proper formatting
    xml_str = ET.tostring(xml_element, encoding='unicode', method='xml')
    
    # Add indentation
    from xml.dom import minidom
    pretty_xml = minidom.parseString(xml_str).toprettyxml(indent='  ')
    
    # Print the pretty XML
    print(pretty_xml) 