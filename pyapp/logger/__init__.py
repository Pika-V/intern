"""
Logging Module
Provides logging utilities for the application
"""

import sys
from loguru import logger
from typing import Optional


class Logger:
    """Logger configuration and management"""
    
    def __init__(self, log_mode: str = "file", log_level: str = "debug"):
        self.log_mode = log_mode
        self.log_level = log_level
        self.setup_logger()
    
    def setup_logger(self):
        """Setup logger configuration"""
        # Remove default handler
        logger.remove()
        
        # Console handler
        if self.log_mode in ["console", "both"]:
            logger.add(
                sys.stdout,
                level=self.log_level.upper(),
                format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
                colorize=True
            )
        
        # File handler
        if self.log_mode in ["file", "both"]:
            logger.add(
                "logs/app_{time:YYYYMMDD}.log",
                level=self.log_level.upper(),
                format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
                rotation="1 day",
                retention="30 days",
                compression="zip",
                encoding="utf-8"
            )
    
    def debug(self, message: str, **kwargs):
        """Log debug message"""
        logger.debug(message, **kwargs)
    
    def info(self, message: str, **kwargs):
        """Log info message"""
        logger.info(message, **kwargs)
    
    def warning(self, message: str, **kwargs):
        """Log warning message"""
        logger.warning(message, **kwargs)
    
    def error(self, message: str, **kwargs):
        """Log error message"""
        logger.error(message, **kwargs)
    
    def critical(self, message: str, **kwargs):
        """Log critical message"""
        logger.critical(message, **kwargs)


# Global logger instance
app_logger = Logger()


def get_logger(name: Optional[str] = None) -> Logger:
    """Get logger instance"""
    if name:
        return logger.bind(name=name)
    return app_logger


# Convenience functions
def LOG_DEBUG(message: str, *args):
    """Log debug message"""
    app_logger.debug(message, *args)


def LOG_INFO(message: str, *args):
    """Log info message"""
    app_logger.info(message, *args)


def LOG_WARNING(message: str, *args):
    """Log warning message"""
    app_logger.warning(message, *args)


def LOG_ERROR(message: str, *args):
    """Log error message"""
    app_logger.error(message, *args)


def LOG_CRITICAL(message: str, *args):
    """Log critical message"""
    app_logger.critical(message, *args)