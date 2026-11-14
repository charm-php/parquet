<?php

declare(strict_types=1);

namespace Charm\Parquet;

enum PageType: int
{
    case DataPage = 0;
    case IndexPage = 1;
    case DictionaryPage = 2;
    case DataPageV2 = 3;
}
