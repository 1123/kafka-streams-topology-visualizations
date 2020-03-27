package org.example.kstopologyvisualizations;

import lombok.Data;

@Data
class Foo {

    String field1;
    String barId;

    String join(Bar bar) {
        return "joined";
    }

}
