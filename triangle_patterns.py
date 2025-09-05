#!/usr/bin/env python3
"""
Python program to print different triangle patterns
"""

def print_right_triangle(height):
    """Print a right triangle using asterisks"""
    print(f"\nRight Triangle (height={height}):")
    for i in range(1, height + 1):
        print('*' * i)

def print_inverted_triangle(height):
    """Print an inverted right triangle"""
    print(f"\nInverted Triangle (height={height}):")
    for i in range(height, 0, -1):
        print('*' * i)

def print_centered_triangle(height):
    """Print a centered triangle"""
    print(f"\nCentered Triangle (height={height}):")
    for i in range(1, height + 1):
        spaces = ' ' * (height - i)
        stars = '*' * (2 * i - 1)
        print(spaces + stars)

def print_number_triangle(height):
    """Print a number triangle"""
    print(f"\nNumber Triangle (height={height}):")
    for i in range(1, height + 1):
        for j in range(1, i + 1):
            print(j, end=' ')
        print()  # New line after each row

def print_floyd_triangle(height):
    """Print Floyd's triangle"""
    print(f"\nFloyd's Triangle (height={height}):")
    num = 1
    for i in range(1, height + 1):
        for j in range(i):
            print(num, end=' ')
            num += 1
        print()  # New line after each row

def print_pascal_triangle(height):
    """Print Pascal's triangle"""
    print(f"\nPascal's Triangle (height={height}):")
    
    def binomial_coefficient(n, k):
        if k > n - k:
            k = n - k
        result = 1
        for i in range(k):
            result = result * (n - i) // (i + 1)
        return result
    
    for i in range(height):
        # Print spaces for centering
        print(' ' * (height - i - 1), end='')
        
        # Print numbers
        for j in range(i + 1):
            print(binomial_coefficient(i, j), end=' ')
        print()  # New line

def main():
    """Main function to demonstrate different triangle patterns"""
    print("=" * 50)
    print("TRIANGLE PATTERNS IN PYTHON")
    print("=" * 50)
    
    height = 5  # Default height
    
    # Print different triangle patterns
    print_right_triangle(height)
    print_inverted_triangle(height)
    print_centered_triangle(height)
    print_number_triangle(height)
    print_floyd_triangle(height)
    print_pascal_triangle(height)
    
    print("\n" + "=" * 50)
    print("You can modify the 'height' variable to change the size!")

if __name__ == "__main__":
    main()
