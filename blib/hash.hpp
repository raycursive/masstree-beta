#ifndef HASH
#define HASH

#include "../str.hh"

/*!
 * Hash the given value depending on its type. 
 * This function is made to be template-specialized. 
 * \param T The type of value to hash. 
 * \param value The value to hash. 
 */
template<typename T>
int hash(T value);

/*!
 * Specialization of hash for the int type. 
 */
template<>
inline int hash<int>(int value){
    return value;
}

template<>
inline int hash<Str>(Str value) {
    std::string s(value.data());
    return stoi(s);
}

#endif