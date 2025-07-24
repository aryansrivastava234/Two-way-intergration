"""
Integration manager - Updated to use custom Stripe integration.
"""
import logging

logger = logging.getLogger(__name__)


class IntegrationManager:
    """Integration manager using working custom Stripe integration."""
    
    def __init__(self):
        self.integrations = {}
        self._setup_stripe()
    
    def _setup_stripe(self):
        """Setup Stripe integration with custom implementation."""
        
        try:
            from .stripe_integration import CustomStripeIntegration, MockStripeIntegration
            from config import settings
            
            # Check if we have a valid API key
            has_valid_key = (
                hasattr(settings, 'stripe') and 
                settings.stripe.secret_key and 
                settings.stripe.secret_key.startswith('sk_')
            )
            
            if has_valid_key:
                # Use custom Stripe integration (working!)
                stripe_config = {
                    'secret_key': settings.stripe.secret_key,
                    'webhook_secret': settings.stripe.webhook_secret,
                    'api_version': settings.stripe.api_version
                }
                
                custom_integration = CustomStripeIntegration(stripe_config)
                
                # Test if it works
                if custom_integration.test_connection():
                    self.integrations['stripe'] = custom_integration
                    logger.info(" Custom Stripe integration initialized and working!")
                else:
                    logger.info(" Custom Stripe test failed, using mock")
                    self.integrations['stripe'] = MockStripeIntegration({})
            else:
                logger.info(" No valid API key, using mock Stripe")
                self.integrations['stripe'] = MockStripeIntegration({})
                
        except Exception as e:
            logger.error(f" Error setting up Stripe integration: {e}")
            # Final fallback to mock
            from .stripe_integration import MockStripeIntegration
            self.integrations['stripe'] = MockStripeIntegration({})
            logger.info(" Fallback to mock Stripe integration")
    
    def get_integration(self, name: str):
        """Get integration by name."""
        return self.integrations.get(name)
    
    def test_all_connections(self):
        """Test all integration connections."""
        results = {}
        for name, integration in self.integrations.items():
            try:
                results[name] = integration.test_connection()
            except Exception as e:
                logger.error(f"Connection test failed for {name}: {e}")
                results[name] = False
        return results
    
    def list_integrations(self):
        """List all integrations."""
        return {
            name: integration.__class__.__name__ 
            for name, integration in self.integrations.items()
        }


# Global instance
integration_manager = IntegrationManager()