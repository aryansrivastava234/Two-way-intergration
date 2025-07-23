"""
CRUD operations for Customer model.
"""
from typing import List, Optional
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from app.models.customer import Customer
from app.schemas import CustomerCreate, CustomerUpdate


class CustomerCRUD:
    """Customer CRUD operations."""

    def create(self, db: Session, customer_data: CustomerCreate) -> Customer:
        """Create a new customer."""
        try:
            customer = Customer(
                name=customer_data.name,
                email=customer_data.email
            )
            db.add(customer)
            db.commit()
            db.refresh(customer)
            return customer
        except IntegrityError:
            db.rollback()
            raise ValueError(f"Customer with email {customer_data.email} already exists")

    def get_by_id(self, db: Session, customer_id: int) -> Optional[Customer]:
        """Get customer by ID."""
        return db.query(Customer).filter(Customer.id == customer_id).first()

    def get_by_email(self, db: Session, email: str) -> Optional[Customer]:
        """Get customer by email."""
        return db.query(Customer).filter(Customer.email == email).first()

    def get_by_stripe_id(self, db: Session, stripe_id: str) -> Optional[Customer]:
        """Get customer by Stripe ID."""
        return db.query(Customer).filter(Customer.stripe_customer_id == stripe_id).first()

    def get_all(self, db: Session, skip: int = 0, limit: int = 100) -> List[Customer]:
        """Get all customers with pagination."""
        return db.query(Customer).offset(skip).limit(limit).all()

    def update(self, db: Session, customer_id: int, customer_data: CustomerUpdate) -> Optional[Customer]:
        """Update customer."""
        customer = self.get_by_id(db, customer_id)
        if not customer:
            return None

        update_data = customer_data.model_dump(exclude_unset=True)
        for field, value in update_data.items():
            setattr(customer, field, value)

        try:
            db.commit()
            db.refresh(customer)
            return customer
        except IntegrityError:
            db.rollback()
            raise ValueError("Email already exists")

    def delete(self, db: Session, customer_id: int) -> bool:
        """Delete customer."""
        customer = self.get_by_id(db, customer_id)
        if not customer:
            return False

        db.delete(customer)
        db.commit()
        return True

    def update_stripe_id(self, db: Session, customer_id: int, stripe_id: str) -> Optional[Customer]:
        """Update customer's Stripe ID."""
        customer = self.get_by_id(db, customer_id)
        if customer:
            customer.stripe_customer_id = stripe_id
            db.commit()
            db.refresh(customer)
        return customer


# Create global instance
customer_crud = CustomerCRUD()