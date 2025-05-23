import csv
import io
import os
import logging
import re
from decimal import Decimal

logging.basicConfig(filename="app.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class API:
    def load_csv(self, csv_content, field_mapping=None, action_type=None, headers_only=False):
        """
        Load and parse CSV content, optionally sanitizing values based on action_type.
        
        Args:
            csv_content (str): The raw CSV content as a string.
            field_mapping (dict, optional): Mapping of CSV fields to Zendesk fields.
            action_type (str, optional): The operation type ('import', 'update', 'delete', 'export').
            headers_only (bool, optional): If True, indicates only headers are needed (no data rows processing).
        
        Returns:
            dict: Contains success status, fields (headers), data_rows, row_count, and message.
        """
        try:
            logger.info(f"load_csv called with headers_only={headers_only}, content length={len(csv_content)}")
            if not csv_content.strip():
                logger.error("CSV content is empty or unreadable")
                return {"success": False, "message": "CSV file is empty or unreadable."}
            
            # Remove BOM and normalize line endings
            csv_content = csv_content.lstrip('\ufeff').replace('\r\n', '\n').replace('\r', '\n')
            csv_file = io.StringIO(csv_content)
            
            # Try to detect delimiter with larger sample and stricter validation
            try:
                sample = csv_content[:8192]
                dialect = csv.Sniffer().sniff(sample, delimiters=',;')
                if not dialect.delimiter:
                    raise csv.Error("Delimiter detection failed.")
                # Validate delimiter consistency
                if sample.count(dialect.delimiter) < sample.count('\n'):
                    raise csv.Error("Delimiter appears inconsistent with row structure.")
            except csv.Error as e:
                logger.warning(f"Delimiter detection failed: {str(e)}. Defaulting to comma.")
                dialect = csv.excel
            
            # Use csv.reader with strict mode and explicit quoting
            reader = csv.reader(
                csv_file,
                dialect,
                quoting=csv.QUOTE_MINIMAL,
                strict=True,
                doublequote=True,
                escapechar='\\'
            )
            headers = next(reader, None)
            
            if not headers or all(h.strip() == "" for h in headers):
                logger.error("No valid headers found in CSV")
                return {"success": False, "message": "CSV file has no valid headers."}
            
            # Clean headers and trim trailing empty columns
            headers = [h.strip().replace('"', '').replace('\\', '') for h in headers if h.strip()]
            if not headers:
                logger.error("No valid headers after cleaning")
                return {"success": False, "message": "No valid headers after cleaning."}
            
            # Initialize data_rows
            data_rows = []
            
            # Process data rows only if headers_only is False
            if not headers_only:
                # Identify the column mapped to html_body
                html_body_column = None
                if field_mapping:
                    for csv_field, zendesk_field in field_mapping.items():
                        if zendesk_field == "html_body":
                            html_body_column = csv_field
                            logger.debug(f"Identified html_body column: {html_body_column}")
                            break
                
                for row in reader:
                    # Trim row to match header length or handle trailing commas
                    if len(row) < len(headers):
                        logger.warning(f"Row has fewer columns than headers: {row}")
                        continue
                    row = row[:len(headers)]
                    cleaned_row = []
                    for i, value in enumerate(row):
                        # Skip sanitization for import/update/delete
                        if action_type != "export":
                            cleaned_row.append(value)
                        else:
                            # Sanitize for export, preserving HTML for html_body column
                            is_html_body = headers[i] == html_body_column if html_body_column else False
                            cleaned_value = self._sanitize_value(value, preserve_html=is_html_body)
                            cleaned_row.append(cleaned_value)
                    data_rows.append(cleaned_row)
            
            logger.info(f"Successfully loaded CSV headers: {headers}")
            return {
                "success": True,
                "fields": headers,
                "data_rows": data_rows,
                "row_count": len(data_rows)
            }
        except Exception as e:
            logger.error(f"Error processing CSV: {str(e)}")
            return {"success": False, "message": f"Error processing CSV: {str(e)}"}
            

    def _sanitize_value(self, value, preserve_html=False):
        """
        Sanitize a single CSV value for export, preserving HTML tags if specified.
        
        Args:
            value: The value to sanitize.
            preserve_html (bool): Whether to preserve HTML tags (e.g., for html_body).
        
        Returns:
            str: Sanitized value.
        """
        if value is None:
            return ""
        if not isinstance(value, str):
            value = str(value)
        
        try:
            # Handle large numeric IDs in scientific notation
            if re.match(r'^\d+\.\d+E\+\d+$', value):
                try:
                    return str(int(float(value)))
                except (ValueError, OverflowError):
                    logger.warning(f"Could not convert scientific notation value: {value}")
            
            # Remove control characters and normalize spaces
            value = re.sub(r'[\x00-\x1F\x7F]', ' ', value)
            value = re.sub(r'\s+', ' ', value).strip()
            
            # Preserve HTML tags if preserve_html is True (for html_body)
            if not preserve_html:
                value = re.sub(r'<[^>]+>', '', value)
            
            # Handle nested quotes and escaped characters
            value = value.replace('""', '"').replace('\\"', '"')
            
            # Ensure valid UTF-8 encoding
            value = value.encode('utf-8', errors='replace').decode('utf-8')
            
            # Truncate extremely long values to prevent API issues
            if len(value) > 65000:
                logger.warning(f"Truncating value longer than 65000 characters: {value[:50]}...")
                value = value[:65000]
            
            return value
        except Exception as e:
            logger.error(f"Error sanitizing value '{value[:50]}...': {str(e)}")
            return value

    def remove_duplicates_from_csv(self, filename):
        """
        Remove duplicate rows from a CSV file, preserving the first occurrence.
        
        Args:
            filename (str): Path to the CSV file.
        
        Returns:
            dict: Success status and message.
        """
        try:
            if not os.path.exists(filename):
                with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
                    writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL, escapechar='\\')
                    writer.writerow([])
                return {"success": True, "message": f"Created empty file: {filename}"}

            headers = None
            unique_rows = []
            seen_rows = set()

            with open(filename, 'r', newline='', encoding='utf-8-sig') as f:
                sample = f.read(8192)
                f.seek(0)
                try:
                    dialect = csv.Sniffer().sniff(sample, delimiters=',;')
                except csv.Error:
                    dialect = csv.excel
                reader = csv.reader(
                    f,
                    dialect,
                    quotechar='"',
                    skipinitialspace=True,
                    quoting=csv.QUOTE_MINIMAL,
                    doublequote=True,
                    escapechar='\\'
                )
                headers = next(reader, None)
                if not headers:
                    return {"success": False, "message": "No headers found in CSV."}

                headers = [h.strip() for h in headers if h.strip()]
                tags_index = headers.index('tags') if 'tags' in headers else -1

                for row in reader:
                    if len(row) < len(headers):
                        logger.warning(f"Skipping row due to column mismatch: {row}")
                        continue
                    row = row[:len(headers)]
                    sanitized_row = tuple(
                        self._sanitize_value(
                            v,
                            preserve_html=False,  # No HTML preservation needed for output CSVs
                            max_length=65000,
                            is_tags_field=(i == tags_index)
                        )
                        for i, v in enumerate(row)
                    )
                    if sanitized_row not in seen_rows:
                        seen_rows.add(sanitized_row)
                        unique_rows.append(row)
                    else:
                        logger.debug(f"Skipping duplicate row: {row}")

            with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(
                    f,
                    delimiter=',',
                    quotechar='"',
                    quoting=csv.QUOTE_MINIMAL,
                    doublequote=True,
                    escapechar='\\'
                )
                writer.writerow(headers)
                writer.writerows(unique_rows)

            return {"success": True, "message": f"Removed duplicates. {len(unique_rows)} rows written."}
        except Exception as e:
            logger.error(f"Error removing duplicates: {str(e)}")
            return {"success": False, "message": f"Error processing CSV: {str(e)}"}

    def _sanitize_csv_value(self, value, strip_html=True, max_length=65000, is_tags_field=False):
        """
        Sanitize a value for CSV export, preserving tags field formatting.
        
        Args:
            value: The value to sanitize.
            strip_html (bool): Whether to strip HTML tags.
            max_length (int): Maximum length for the sanitized value.
            is_tags_field (bool): Whether the value is from the tags field.
        
        Returns:
            str: Sanitized value.
        """
        if value is None or value == "" or value == "null":
            return ""
        if isinstance(value, list):
            value = ",".join(str(v).strip() for v in value if v)
            if not value:
                return ""
        if not isinstance(value, str):
            try:
                if isinstance(value, (int, float, Decimal)):
                    value = f"{value:.0f}" if isinstance(value, (float, Decimal)) and value.is_integer() else str(value)
                else:
                    value = str(value)
            except Exception as e:
                logger.warning(f"Error converting value to string: {value}. Error: {e}")
                value = ""
        
        try:
            # Handle scientific notation
            if re.match(r'^-?\d+\.\d+E[+-]\d+$', value, re.IGNORECASE):
                try:
                    sanitized_value = f"{Decimal(value):.0f}"
                    return sanitized_value
                except (ValueError, OverflowError):
                    logger.warning(f"Could not convert scientific notation value: {value}")
            
            # Remove control characters
            value = re.sub(r'[\x00-\x08\x0B-\x0C\x0E-\x1F\x7F]', ' ', value)
            
            # Strip HTML if requested
            if strip_html:
                value = re.sub(r'<[^>]+>', '', value)
            
            # Normalize whitespace
            value = re.sub(r'\s+', ' ', value).strip()
            
            # Truncate long values
            if len(value) > max_length:
                logger.warning(f"Truncating value longer than {max_length} characters: {value[:50]}...")
                value = value[:max_length]
            
            # Validate UTF-8
            try:
                value.encode('utf-8').decode('utf-8')
            except (UnicodeEncodeError, UnicodeDecodeError):
                logger.warning(f"Invalid UTF-8 characters in value: {value[:50]}...")
                value = value.encode('utf-8', errors='replace').decode('utf-8')
            
            # Handle tags field
            if is_tags_field or not any(char in value for char in ',"\n\r'):
                return value
            
            return value
        except Exception as e:
            logger.error(f"Error sanitizing CSV value '{value[:50]}...': {e}")
            return ""