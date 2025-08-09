import csv
import os
from typing import Dict, Optional, Tuple


def log_message(msg: str):
    """Simple logging function to avoid dependency issues"""
    print(f"[TICKER] {msg}")


class IndianTickerMapper:
    """
    Service to map BSE codes to Yahoo Finance symbols using the comprehensive ticker database
    """
    
    def __init__(self):
        self.bse_to_yahoo: Dict[str, str] = {}
        self.yahoo_to_bse: Dict[str, str] = {}
        self.company_names: Dict[str, str] = {}
        self.loaded = False
        
    def load_ticker_data(self) -> bool:
        """Load ticker mapping from CSV file"""
        if self.loaded:
            return True
            
        try:
            csv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'indian_stock_tickers.csv')
            
            if not os.path.exists(csv_path):
                log_message(f"[TICKER] CSV file not found at {csv_path}")
                return False
                
            with open(csv_path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                count = 0
                
                for row in reader:
                    yahoo_symbol = row.get('Yahoo Symbol', '').strip()
                    company_name = row.get('Company Name', '').strip()
                    bse_code = row.get('BSE Code', '').strip()
                    
                    if yahoo_symbol and bse_code:
                        # Map BSE code to Yahoo symbol
                        self.bse_to_yahoo[bse_code] = yahoo_symbol
                        self.yahoo_to_bse[yahoo_symbol] = bse_code
                        
                        # Store company names for both codes
                        if company_name:
                            self.company_names[bse_code] = company_name
                            self.company_names[yahoo_symbol] = company_name
                        
                        count += 1
                        
            self.loaded = True
            log_message(f"[TICKER] Loaded {count} ticker mappings successfully")
            return True
            
        except Exception as e:
            log_message(f"[TICKER] Error loading ticker data: {e}")
            return False
    
    def get_yahoo_symbol(self, bse_code: str) -> Optional[str]:
        """
        Convert BSE code to Yahoo Finance symbol
        
        Args:
            bse_code: BSE scrip code (e.g., "500325")
            
        Returns:
            Yahoo symbol (e.g., "RELIANCE.NS") or None if not found
        """
        if not self.loaded:
            self.load_ticker_data()
            
        # Clean the BSE code
        clean_bse = str(bse_code).strip()
        
        # First try direct mapping
        yahoo_symbol = self.bse_to_yahoo.get(clean_bse)
        if yahoo_symbol:
            return yahoo_symbol
            
        # Try with .BO suffix (some might be stored this way)
        bo_symbol = f"{clean_bse}.BO"
        if bo_symbol in self.yahoo_to_bse:
            return bo_symbol
            
        # If not found, try common patterns
        # Most major stocks are on NSE (.NS)
        ns_symbol = f"{clean_bse}.NS"
        if self._symbol_exists_in_data(ns_symbol):
            return ns_symbol
            
        # Try BSE format (.BO)
        bo_symbol = f"{clean_bse}.BO"
        if self._symbol_exists_in_data(bo_symbol):
            return bo_symbol
            
        return None
    
    def get_bse_code(self, yahoo_symbol: str) -> Optional[str]:
        """
        Convert Yahoo Finance symbol to BSE code
        
        Args:
            yahoo_symbol: Yahoo symbol (e.g., "RELIANCE.NS")
            
        Returns:
            BSE code (e.g., "500325") or None if not found
        """
        if not self.loaded:
            self.load_ticker_data()
            
        return self.yahoo_to_bse.get(yahoo_symbol.strip())
    
    def get_company_name(self, code: str) -> Optional[str]:
        """
        Get company name for BSE code or Yahoo symbol
        
        Args:
            code: BSE code or Yahoo symbol
            
        Returns:
            Company name or None if not found
        """
        if not self.loaded:
            self.load_ticker_data()
            
        return self.company_names.get(code.strip())
    
    def find_best_yahoo_symbol(self, bse_code: str) -> Tuple[Optional[str], str]:
        """
        Find the best Yahoo symbol for a BSE code with preference logic
        
        Args:
            bse_code: BSE scrip code
            
        Returns:
            Tuple of (yahoo_symbol, exchange_preference)
            exchange_preference: "direct_mapping", "NSE", "BSE", or "not_found"
        """
        if not self.loaded:
            self.load_ticker_data()
            
        clean_bse = str(bse_code).strip()
        
            # 1. Try direct mapping first (highest confidence). Our CSV uses NSE .NS.
        direct_symbol = self.bse_to_yahoo.get(clean_bse)
        if direct_symbol:
                if direct_symbol.endswith('.NS'):
                    return direct_symbol, "NSE"
                if direct_symbol.endswith('.BO'):
                    # Derive NSE symbol when data mistakenly carries .BO
                    return direct_symbol[:-3] + '.NS', "NSE"
                return direct_symbol + '.NS' if '.' not in direct_symbol else direct_symbol, "NSE"

        # 2. Try to find any NSE symbol in our reverse map for the same BSE code
        for symbol in self.yahoo_to_bse:
            if self.yahoo_to_bse[symbol] == clean_bse and symbol.endswith('.NS'):
                return symbol, "NSE"

        # 3. Try to find any BSE symbol in our reverse map for the same BSE code and derive NSE
        for symbol in self.yahoo_to_bse:
            if self.yahoo_to_bse[symbol] == clean_bse and symbol.endswith('.BO'):
                derived_nse = symbol[:-3] + '.NS'
                return derived_nse, "NSE"

        # 4. Not found in our data
        return None, "not_found"
    
    def _symbol_exists_in_data(self, symbol: str) -> bool:
        """Check if symbol exists in our loaded data"""
        return symbol in self.yahoo_to_bse
    
    def get_stats(self) -> Dict[str, int]:
        """Get statistics about loaded ticker data"""
        if not self.loaded:
            self.load_ticker_data()
            
        nse_count = sum(1 for symbol in self.yahoo_to_bse if symbol.endswith('.NS'))
        bse_count = sum(1 for symbol in self.yahoo_to_bse if symbol.endswith('.BO'))
        
        return {
            "total_mappings": len(self.bse_to_yahoo),
            "nse_symbols": nse_count,
            "bse_symbols": bse_count,
            "companies_with_names": len(self.company_names)
        }
    
    def search_by_company_name(self, company_name: str, limit: int = 5) -> list:
        """
        Search for stocks by company name (fuzzy search)
        
        Args:
            company_name: Partial company name to search
            limit: Maximum results to return
            
        Returns:
            List of (bse_code, yahoo_symbol, company_name) tuples
        """
        if not self.loaded:
            self.load_ticker_data()
            
        search_term = company_name.lower().strip()
        results = []
        
        for code, name in self.company_names.items():
            if search_term in name.lower():
                # Get the corresponding codes
                if code in self.bse_to_yahoo:  # code is BSE
                    bse_code = code
                    yahoo_symbol = self.bse_to_yahoo[code]
                elif code in self.yahoo_to_bse:  # code is Yahoo
                    yahoo_symbol = code
                    bse_code = self.yahoo_to_bse[code]
                else:
                    continue
                    
                results.append((bse_code, yahoo_symbol, name))
                
                if len(results) >= limit:
                    break
                    
        return results


# Global instance for easy access
ticker_mapper = IndianTickerMapper()


def get_yahoo_symbol_for_bse(bse_code: str) -> Optional[str]:
    """
    Convenience function to get Yahoo symbol for BSE code
    
    Args:
        bse_code: BSE scrip code
        
    Returns:
        Yahoo Finance symbol or None
    """
    return ticker_mapper.get_yahoo_symbol(bse_code)


def get_best_yahoo_symbol(bse_code: str) -> Tuple[Optional[str], str]:
    """
    Convenience function to get best Yahoo symbol with preference info
    
    Args:
        bse_code: BSE scrip code
        
    Returns:
        Tuple of (yahoo_symbol, exchange_info)
    """
    return ticker_mapper.find_best_yahoo_symbol(bse_code)


def get_company_name_for_code(code: str) -> Optional[str]:
    """
    Convenience function to get company name
    
    Args:
        code: BSE code or Yahoo symbol
        
    Returns:
        Company name or None
    """
    return ticker_mapper.get_company_name(code)


def search_stocks_by_name(company_name: str, limit: int = 5) -> list:
    """
    Convenience function to search stocks by company name
    
    Args:
        company_name: Partial company name
        limit: Maximum results
        
    Returns:
        List of (bse_code, yahoo_symbol, company_name) tuples
    """
    return ticker_mapper.search_by_company_name(company_name, limit)
