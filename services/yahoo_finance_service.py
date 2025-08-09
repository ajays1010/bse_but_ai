import yfinance as yf
from typing import Dict, Any, Optional
from .ticker_mapping_service import get_best_yahoo_symbol, get_company_name_for_code, ticker_mapper
from .market_fallbacks import fetch_market_data_via_gemini
import pandas as pd


def log_message(msg: str):
    """Simple logging function to avoid dependency issues"""
    print(f"[YAHOO] {msg}")


def get_stock_data_with_symbol(yahoo_symbol: str, original_bse_code: str) -> Dict[str, Any]:
    """
    Helper function to fetch data with a specific Yahoo symbol
    """
    try:
        ticker = yf.Ticker(yahoo_symbol)
        info = ticker.info
        hist = ticker.history(period="5d")
        
        # Get company name from our mapping first
        mapped_company_name = get_company_name_for_code(original_bse_code)
        yahoo_company_name = info.get("longName", info.get("shortName", "N/A"))
        final_company_name = mapped_company_name if mapped_company_name else yahoo_company_name
        
        result = {
            "original_bse_code": original_bse_code,
            "yahoo_symbol": yahoo_symbol,
            "company_name": final_company_name,
            "current_price": info.get("currentPrice", info.get("regularMarketPrice", "N/A")),
            "previous_close": info.get("previousClose", "N/A"),
            "day_change": None,
            "day_change_percent": None,
            "market_cap": info.get("marketCap", "N/A"),
        }
        
        # Calculate day change
        if result["current_price"] != "N/A" and result["previous_close"] != "N/A":
            try:
                current = float(result["current_price"])
                previous = float(result["previous_close"])
                change = current - previous
                change_percent = (change / previous) * 100
                result["day_change"] = round(change, 2)
                result["day_change_percent"] = round(change_percent, 2)
            except:
                pass
                
        return result
        
    except Exception as e:
        return {"error": str(e)}


def get_stock_data_yahoo(scrip_code: str) -> Dict[str, Any]:
    """
    Fetch comprehensive stock data from Yahoo Finance
    Returns live market data including price, changes, financials, and ratios
    """
    try:
        # Use comprehensive ticker mapping for accurate symbol conversion
        yahoo_symbol, exchange_preference = get_best_yahoo_symbol(scrip_code)
        
        if not yahoo_symbol:
            log_message(f"[YAHOO] No mapping found for BSE code {scrip_code}")
            return {
                "symbol": scrip_code,
                "error": f"No Yahoo Finance mapping found for {scrip_code}",
                "current_price": "N/A",
                "company_name": "Mapping not available"
            }
        
        log_message(f"[YAHOO] Fetching data for {yahoo_symbol} (BSE: {scrip_code}, Exchange: {exchange_preference})")
        
        # Create ticker object
        ticker = yf.Ticker(yahoo_symbol)
        
        # Get basic info
        info = ticker.info
        
        # Get historical data for trend analysis
        hist = ticker.history(period="5d")
        
        # Get quarterly financials
        quarterly_financials = ticker.quarterly_financials
        quarterly_balance_sheet = ticker.quarterly_balance_sheet
        quarterly_cashflow = ticker.quarterly_cashflow
        
        # Get company name from our mapping first, then Yahoo data
        mapped_company_name = get_company_name_for_code(scrip_code)
        yahoo_company_name = info.get("longName", info.get("shortName", "N/A"))
        
        # Use mapped name if available, otherwise Yahoo name
        final_company_name = mapped_company_name if mapped_company_name else yahoo_company_name
        
        # Extract key data
        result = {
            "original_bse_code": scrip_code,
            "yahoo_symbol": yahoo_symbol,
            "exchange_preference": exchange_preference,
            "company_name": final_company_name,
            "sector": info.get("sector", "N/A"),
            "industry": info.get("industry", "N/A"),
            
            # LIVE PRICE DATA
            "current_price": info.get("currentPrice", info.get("regularMarketPrice", "N/A")),
            "previous_close": info.get("previousClose", "N/A"),
            "day_change": None,
            "day_change_percent": None,
            "52_week_high": info.get("fiftyTwoWeekHigh", "N/A"),
            "52_week_low": info.get("fiftyTwoWeekLow", "N/A"),
            
            # MARKET DATA
            "market_cap": info.get("marketCap", "N/A"),
            "volume": info.get("volume", info.get("regularMarketVolume", "N/A")),
            "avg_volume": info.get("averageVolume", "N/A"),
            "pe_ratio": info.get("trailingPE", "N/A"),
            "forward_pe": info.get("forwardPE", "N/A"),
            "pb_ratio": info.get("priceToBook", "N/A"),
            "dividend_yield": info.get("dividendYield", "N/A"),
            
            # FINANCIAL METRICS
            "revenue_ttm": info.get("totalRevenue", "N/A"),
            "gross_profit": info.get("grossProfits", "N/A"),
            "net_income": info.get("netIncomeToCommon", "N/A"),
            "operating_income": info.get("operatingIncome", "N/A"),
            "eps_ttm": info.get("trailingEps", "N/A"),
            "eps_forward": info.get("forwardEps", "N/A"),
            "total_debt": info.get("totalDebt", "N/A"),
            "total_cash": info.get("totalCash", "N/A"),
            "debt_to_equity": info.get("debtToEquity", "N/A"),
            "return_on_equity": info.get("returnOnEquity", "N/A"),
            "return_on_assets": info.get("returnOnAssets", "N/A"),
            
            # VALUATION & GROWTH
            "book_value": info.get("bookValue", "N/A"),
            "price_to_sales": info.get("priceToSalesTrailing12Months", "N/A"),
            "earnings_growth": info.get("earningsGrowth", "N/A"),
            "revenue_growth": info.get("revenueGrowth", "N/A"),
            
            # ANALYST DATA
            "target_mean_price": info.get("targetMeanPrice", "N/A"),
            "target_high_price": info.get("targetHighPrice", "N/A"),
            "target_low_price": info.get("targetLowPrice", "N/A"),
            "recommendation_mean": info.get("recommendationMean", "N/A"),
            "recommendation_key": info.get("recommendationKey", "N/A"),
            
            # TREND ANALYSIS
            "trend_5d": "N/A",
            "volatility": "N/A"
        }
        
        # Calculate day change
        if result["current_price"] != "N/A" and result["previous_close"] != "N/A":
            try:
                current = float(result["current_price"])
                previous = float(result["previous_close"])
                change = current - previous
                change_percent = (change / previous) * 100
                result["day_change"] = round(change, 2)
                result["day_change_percent"] = round(change_percent, 2)
            except:
                pass
        
        # Calculate 5-day trend
        if not hist.empty and len(hist) >= 2:
            try:
                start_price = hist['Close'].iloc[0]
                end_price = hist['Close'].iloc[-1]
                trend_change = ((end_price - start_price) / start_price) * 100
                if trend_change > 2:
                    result["trend_5d"] = "STRONG UP"
                elif trend_change > 0:
                    result["trend_5d"] = "UP"
                elif trend_change < -2:
                    result["trend_5d"] = "STRONG DOWN"
                elif trend_change < 0:
                    result["trend_5d"] = "DOWN"
                else:
                    result["trend_5d"] = "SIDEWAYS"
                    
                # Calculate volatility
                returns = hist['Close'].pct_change().dropna()
                volatility = returns.std() * 100
                result["volatility"] = f"{volatility:.1f}%" if volatility else "N/A"
            except:
                pass
        
        # Format financial numbers for readability
        result = format_financial_numbers(result)
        
        log_message(f"[YAHOO] Successfully fetched data for {yahoo_symbol}")
        return result
        
    except Exception as e:
        log_message(f"[YAHOO] Error fetching data for {yahoo_symbol}: {e}")
        
        # Try alternative exchange if primary fails
        alternative_symbol = None
        if yahoo_symbol.endswith('.BO'):
            alternative_symbol = yahoo_symbol.replace('.BO', '.NS')
        elif yahoo_symbol.endswith('.NS'):
            alternative_symbol = yahoo_symbol.replace('.NS', '.BO')
        
        if alternative_symbol:
            try:
                log_message(f"[YAHOO] Trying alternative exchange: {alternative_symbol}")
                # Recursively call with the alternative symbol
                alt_result = get_stock_data_with_symbol(alternative_symbol, scrip_code)
                if alt_result and not alt_result.get('error'):
                    alt_result['exchange_preference'] = 'alternative'
                    return alt_result
            except Exception as alt_e:
                log_message(f"[YAHOO] Alternative exchange also failed: {alt_e}")
        
        # Fallback 2: Try Gemini web search for market data
        try:
            fallback = fetch_market_data_via_gemini(scrip_code, get_company_name_for_code(scrip_code), yahoo_symbol)
        except Exception:
            fallback = None
        if fallback and (fallback.get("current_price") not in (None, "N/A")):
            company_name = fallback.get("company_name") or get_company_name_for_code(scrip_code) or "Data unavailable"
            return {
                "original_bse_code": scrip_code,
                "yahoo_symbol": yahoo_symbol,
                "error": None,
                "current_price": fallback.get("current_price"),
                "previous_close": fallback.get("previous_close", "N/A"),
                "day_change": None,
                "day_change_percent": fallback.get("day_change_percent", "N/A"),
                "market_cap": fallback.get("market_cap", "N/A"),
                "company_name": company_name,
                "exchange_preference": "gemini_web"
            }

        # Final fallback - return error info
        company_name = get_company_name_for_code(scrip_code) or "Data unavailable"
        return {
            "original_bse_code": scrip_code,
            "yahoo_symbol": yahoo_symbol,
            "error": str(e),
            "current_price": "N/A",
            "company_name": company_name,
            "day_change": "N/A",
            "day_change_percent": "N/A",
            "exchange_preference": "failed"
        }


def format_financial_numbers(data: Dict[str, Any]) -> Dict[str, Any]:
    """Format large numbers in Indian crore/lakh format"""
    
    def format_number(value):
        if value == "N/A" or value is None:
            return "N/A"
        
        try:
            num = float(value)
            if abs(num) >= 10000000:  # 1 crore
                return f"₹{num/10000000:.1f}Cr"
            elif abs(num) >= 100000:  # 1 lakh
                return f"₹{num/100000:.1f}L"
            elif abs(num) >= 1000:
                return f"₹{num/1000:.1f}K"
            else:
                return f"₹{num:.1f}"
        except:
            return str(value)
    
    # Format specific financial fields
    financial_fields = [
        "market_cap", "revenue_ttm", "gross_profit", "net_income", 
        "operating_income", "total_debt", "total_cash"
    ]
    
    for field in financial_fields:
        if field in data:
            data[field] = format_number(data[field])
    
    return data


def get_stock_recommendation_yahoo(stock_data: Dict[str, Any]) -> str:
    """
    Generate investment recommendation based on Yahoo Finance data
    """
    try:
        rec_key = stock_data.get("recommendation_key", "").upper()
        pe_ratio = stock_data.get("pe_ratio", "N/A")
        day_change_pct = stock_data.get("day_change_percent", "N/A")
        trend_5d = stock_data.get("trend_5d", "N/A")
        
        # Base recommendation from analysts
        base_rec = "HOLD"
        if rec_key in ["STRONG_BUY", "STRONGBUY"]:
            base_rec = "STRONG BUY"
        elif rec_key in ["BUY"]:
            base_rec = "BUY"
        elif rec_key in ["SELL"]:
            base_rec = "SELL"
        elif rec_key in ["STRONG_SELL", "STRONGSELL"]:
            base_rec = "STRONG SELL"
        
        # Add momentum analysis
        momentum = ""
        if day_change_pct != "N/A" and day_change_pct > 3:
            momentum = " - Strong upward momentum"
        elif day_change_pct != "N/A" and day_change_pct < -3:
            momentum = " - Selling pressure evident"
        elif trend_5d == "STRONG UP":
            momentum = " - Building bullish momentum"
        elif trend_5d == "STRONG DOWN":
            momentum = " - Bearish trend continues"
        
        return f"{base_rec}{momentum}"
        
    except Exception as e:
        return "HOLD - Analyze fundamentals carefully"


def test_ticker_integration(test_codes: list = None) -> Dict[str, Any]:
    """
    Test the ticker mapping integration with sample BSE codes
    
    Args:
        test_codes: List of BSE codes to test, if None uses popular stocks
        
    Returns:
        Test results with success/failure info
    """
    if test_codes is None:
        # Popular Indian stocks for testing
        test_codes = ["500325", "500034", "532540", "500209", "532215"]  # Reliance, Bajaj Finance, TCS, Infosys, HDFC Bank
    
    results = {
        "total_tested": len(test_codes),
        "successful_mappings": 0,
        "successful_data_fetch": 0,
        "failed_mappings": [],
        "failed_data_fetch": [],
        "sample_data": {}
    }
    
    log_message(f"[YAHOO_TEST] Testing ticker integration with {len(test_codes)} codes")
    
    for bse_code in test_codes:
        try:
            # Test mapping
            yahoo_symbol, exchange_pref = get_best_yahoo_symbol(bse_code)
            if yahoo_symbol:
                results["successful_mappings"] += 1
                
                # Test data fetch
                stock_data = get_stock_data_yahoo(bse_code)
                if stock_data and not stock_data.get('error'):
                    results["successful_data_fetch"] += 1
                    
                    # Store sample data for first successful fetch
                    if not results["sample_data"]:
                        results["sample_data"] = {
                            "bse_code": bse_code,
                            "yahoo_symbol": yahoo_symbol,
                            "company_name": stock_data.get("company_name", "N/A"),
                            "current_price": stock_data.get("current_price", "N/A"),
                            "exchange_preference": exchange_pref
                        }
                else:
                    results["failed_data_fetch"].append({
                        "bse_code": bse_code,
                        "yahoo_symbol": yahoo_symbol,
                        "error": stock_data.get('error', 'Unknown error')
                    })
            else:
                results["failed_mappings"].append(bse_code)
                
        except Exception as e:
            results["failed_mappings"].append(f"{bse_code}: {str(e)}")
    
    # Calculate success rates
    mapping_success_rate = (results["successful_mappings"] / results["total_tested"]) * 100
    data_success_rate = (results["successful_data_fetch"] / results["total_tested"]) * 100
    
    results["mapping_success_rate"] = round(mapping_success_rate, 1)
    results["data_success_rate"] = round(data_success_rate, 1)
    
    log_message(f"[YAHOO_TEST] Mapping success: {mapping_success_rate:.1f}%, Data fetch success: {data_success_rate:.1f}%")
    
    return results


def get_ticker_mapping_stats() -> Dict[str, Any]:
    """
    Get statistics about the loaded ticker mapping
    
    Returns:
        Statistics about available mappings
    """
    stats = ticker_mapper.get_stats()
    
    # Add some additional analysis
    stats["data_source"] = "indian_stock_tickers.csv"
    stats["mapping_loaded"] = ticker_mapper.loaded
    
    if ticker_mapper.loaded:
        # Test with a popular stock
        test_result = get_best_yahoo_symbol("500325")  # Reliance
        stats["sample_mapping"] = {
            "bse_code": "500325",
            "yahoo_symbol": test_result[0],
            "exchange": test_result[1],
            "company_name": get_company_name_for_code("500325")
        } if test_result[0] else None
    
    return stats
