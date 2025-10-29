#!/bin/bash

# SSL Certificate Management Script for IFRS9 Pro
# This script handles SSL certificate generation and renewal

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if domain is provided
if [ -z "$1" ]; then
    print_error "Please provide your domain name as an argument"
    echo "Usage: $0 <your-domain.com>"
    exit 1
fi

DOMAIN=$1
EMAIL=${SSL_EMAIL:-"admin@$DOMAIN"}

print_status "Setting up SSL certificate for domain: $DOMAIN"
print_status "Email for Let's Encrypt: $EMAIL"

# Create necessary directories
mkdir -p certbot/conf
mkdir -p certbot/www

# Update nginx configuration with actual domain
print_status "Updating nginx configuration with domain: $DOMAIN"
sed -i "s/your-domain.com/$DOMAIN/g" nginx/conf.d/default.conf

# Start nginx container for certificate generation
print_status "Starting nginx container for certificate generation..."
docker-compose -f docker-compose.prod.yml up -d nginx

# Wait for nginx to be ready
print_status "Waiting for nginx to be ready..."
sleep 10

# Generate SSL certificate
print_status "Generating SSL certificate..."
docker-compose -f docker-compose.prod.yml run --rm certbot

# Test certificate generation
if [ $? -eq 0 ]; then
    print_status "SSL certificate generated successfully!"
    
    # Restart nginx with SSL configuration
    print_status "Restarting nginx with SSL configuration..."
    docker-compose -f docker-compose.prod.yml restart nginx
    
    print_status "SSL setup complete!"
    print_status "Your application should now be accessible at: https://$DOMAIN"
else
    print_error "SSL certificate generation failed!"
    print_error "Please check your domain configuration and try again."
    exit 1
fi

# Create renewal script
print_status "Creating SSL renewal script..."
cat > renew-ssl.sh << 'EOF'
#!/bin/bash
# SSL Certificate Renewal Script

set -e

echo "Renewing SSL certificates..."
docker-compose -f docker-compose.prod.yml run --rm certbot renew
docker-compose -f docker-compose.prod.yml restart nginx
echo "SSL certificates renewed successfully!"
EOF

chmod +x renew-ssl.sh

print_status "SSL renewal script created: renew-ssl.sh"
print_warning "Don't forget to set up a cron job to run renew-ssl.sh monthly!"
print_warning "Example cron job: 0 2 1 * * /path/to/your/project/renew-ssl.sh"
