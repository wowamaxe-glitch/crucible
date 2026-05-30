//! Test utilities for creating domain objects in tests.
//!
//! This module provides factory functions for creating common domain objects
//! used throughout the test suite. Factories help reduce boilerplate and ensure
//! consistent test data creation.
//!
//! # Usage
//!
//! ```rust
//! use backend::test_utils::factories;
//!
//! // Create a user with default values
//! let user = factories::user::create();
//!
//! // Create a user with custom values
//! let user = factories::user::create_with(|u| {
//!     u.email = "test@example.com".to_string();
//!     u.is_active = false;
//! });
//!
//! // Build a user step by step
//! let user = factories::user::build()
//!     .email("custom@example.com")
//!     .is_admin(true)
//!     .finish();
//! ```

pub mod order;
pub mod product;
pub mod session;
pub mod user;

pub use order::{
    build as build_order, create as create_order, create_for_user as create_order_for_user,
    create_many as create_orders, create_many_with as create_orders_with,
    create_with as create_order_with, create_with_items as create_order_with_items, OrderFactory,
    OrderItem, OrderStatus, PaymentMethod,
};
pub use product::{
    build as build_product, create as create_product, create_many as create_products,
    create_many_with as create_products_with, create_with as create_product_with, Availability,
    ProductCategory, ProductFactory,
};
pub use session::{
    build as build_session, create as create_session, create_for_user as create_session_for_user,
    create_many as create_sessions, create_many_for_user as create_sessions_for_user,
    create_many_with as create_sessions_with, create_with as create_session_with, SessionFactory,
    SessionStatus, SessionType,
};
pub use user::{
    build as build_user, create as create_user, create_many as create_users,
    create_many_with as create_users_with, create_with as create_user_with, Role, UserFactory,
};
