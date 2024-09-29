class Person < ApplicationRecord
  validates :name, :email, :age, presence: true
end
